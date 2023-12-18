# venv
python3 -m venv venv
source venv/bin/activate

pip install psycopg2-binary
pip install "apache-airflow[celery]==2.7.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.10.txt"







# === Airflow local ===========================================================================================
export AIRFLOW_HOME=/home/yzpt/projects/carburant_gcp
airflow db init
airflow users create --username admin --firstname Yohann --lastname Zapart --role Admin --email yohann@zapart.com --password admin

# airflow.cfg --> don't load example dags
sed -i 's/load_examples = True/load_examples = False/g' airflow.cfg

# starting scheduler
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)
airflow scheduler

# starting webserver
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)
airflow webserver --port 8080




# === postgresql ===============================================================================================
sudo apt install postgresql

export DB_NAME=carburants_db
export TABLE_NAME=records
export USER_NAME=yzpt_airflow
export PASSWORD=pp

# List all databases
sudo -i -u postgres psql -c "\l"

# Drop database
sudo -i -u postgres psql -c "DROP DATABASE $DB_NAME"

# delete user
sudo -i -u postgres psql -c "DROP USER $USER_NAME"

# list users
sudo -i -u postgres psql -c "\du"

# Create database
sudo -i -u postgres psql -c "CREATE DATABASE $DB_NAME"


sudo -i -u postgres psql <<EOF
CREATE DATABASE $DB_NAME;
CREATE USER $USER_NAME WITH PASSWORD '$PASSWORD';
ALTER ROLE $USER_NAME SET client_encoding TO 'utf8';
ALTER ROLE $USER_NAME SET default_transaction_isolation TO 'read committed';
ALTER ROLE $USER_NAME SET timezone TO 'Europe/Paris';
GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $USER_NAME;
EOF

psql -U yzpt_airflow -d carburants_db


psql -U $USER_NAME $DB_NAME <<EOF
CREATE TABLE IF NOT EXISTS $TABLE_NAME (
    record_timestamp TIMESTAMP,
    id BIGINT,
    latitude REAL,
    longitude REAL,
    cp VARCHAR(50),
    pop TEXT,
    adresse TEXT,
    ville VARCHAR(50),
    horaires TEXT,
    services TEXT,
    gazole_maj TIMESTAMP,
    gazole_prix REAL,
    sp95_maj TIMESTAMP,
    sp95_prix REAL,
    e85_maj TIMESTAMP,
    e85_prix REAL,
    gplc_maj TIMESTAMP,
    gplc_prix REAL,
    e10_maj TIMESTAMP,
    e10_prix REAL,
    sp98_maj TIMESTAMP,
    sp98_prix REAL,
    PRIMARY KEY (record_timestamp, id)
);
EOF



SELECT id, record_timestamp from records ORDER BY record_timestamp DESC LIMIT 5;

sudo -i -u yzpt psql carburants <<EOF > query_results.txt
SELECT * FROM records ORDER BY record_timestamp DESC;
EOF

sudo -i -u yzpt psql carburants
SELECT id, record_timestamp, ville, adresse, latitude, longitude FROM records ORDER BY record_timestamp DESC LIMIT 5;




# === DOCKER ===================================================================================================

# === Airflow Docker ===========================================================================================
# https://airflow.apache.org/docs/docker-stack/index.html
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.3/docker-compose.yaml'
mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker compose up airflow-init
docker compose down --volumes --remove-orphans
docker compose up


git add . && git commit -m "airflow docker" && git push origin docker   







# === GCP Composer & BigQuery ==================================================================================

# gcloud config
PROJECT_ID=carburants-composer
gcloud projects create $PROJECT_ID
gcloud config set project $PROJECT_ID

SERVICE_ACCOUNT_NAME=SA-$PROJECT_ID
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME

# create a key for the service account
gcloud iam service-accounts keys create key-$SERVICE_ACCOUNT_NAME.json --iam-account=$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com

# add the service account to the project
gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com --role roles/owner

# composer roles/composer.ServiceAgentV2Ext permission to the service account
gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com --role roles/composer.ServiceAgentV2Ext
# bigquery & storage admin roles
gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com --role roles/storage.admin
gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com --role roles/bigquery.admin

# === Billing ==================================================================================================
# link the project to the billing account
BILLING_ACCOUNT_ID=$(gcloud billing accounts list --format='value(ACCOUNT_ID)' --filter='NAME="billing_account_2"')
gcloud billing projects link $PROJECT_ID --billing-account $BILLING_ACCOUNT_ID

# enable the required APIs
gcloud services enable composer.googleapis.com

# === Storage
# create a bucket
BUCKET_NAME=carburants-composer-bucket
gsutil mb -l europe-west2 gs://$BUCKET_NAME




# === Composer
# create composer environment
# https://cloud.google.com/composer/docs/how-to/managing/creating

gcloud composer environments create cli-1 \
    --location europe-west2 \
    --image-version composer-1.20.12-airflow-2.4.3 \
    --service-account $SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com \
    --zone europe-west2-c \
    --node-count 3 \
    --scheduler-count 1 \
    --disk-size 50 \
    --machine-type n1-standard-2 \
    --cloud-sql-machine-type db-n1-standard-2 \
    --web-server-machine-type composer-n1-webserver-2




# upload a DAG to the environment
gcloud composer environments storage dags import \
    --environment cli-1 \
    --location europe-west2 \
    --source dags/allo2_dag.py


# === dag dependencies ==========================================================================================
# venv
python3 -m venv venv-composer-dag-2
source venv-composer-dag-2/bin/activate
pip install requests
pip install lxml
# pip install google-cloud-bigquery
# pip install google-cloud-storage
pip freeze > requirements-composer-dag.txt


# RROR: (gcloud.composer.environments.update) Error updating [projects/carburants-composer/locations/europe-west2/environments/cli-1]: Operation [projects/carburants-composer/locations/europe-west2/operations/16312a7d-1ed9-4026-b95a-ace483b07f93] 
# failed: Failed to install Python packages. 
# aiohttp 3.8.3 has requirement charset-normalizer<3.0,>=2.0, but you have charset-normalizer 3.3.2.
#  Check the Cloud Build log at https://console.cloud.google.com/cloud-build/builds/078063ac-a6b3-488b-befd-32d31af24946?project=10289516830 for details. For detailed instructions see https://cloud.google.com/composer/docs/troubleshooting-package-installation
sed -i 's/charset-normalizer==3.3.2/charset-normalizer<3.0,>=2.0/g' requirements-composer-dag.txt


# add packages to the environment
gcloud composer environments update cli-1 \
    --location europe-west2 \
    --update-pypi-packages-from-file requirements-composer-dag.txt



# open the Airflow UI
gcloud composer environments describe cli-1 --location europe-west2 --format="value(config.airflowUri)"


# === BigQuery =================================================================================================
# bq create dataset
bq mk --dataset carburants-composer:test_dataset

# create table
bq mk --table carburants-composer:test_dataset.test_table \
    id:INTEGER,nom:STRING,prénom:STRING

bq mk --schema table_schema_bq.json --table carburants-composer:test_dataset.test_carbu 


# delete table
bq rm -f carburants-composer:test_dataset.test_table



gsutil cp data.zip gs://carburants-composer-bucket/data.zip
gsutil ls gs://carburants-composer-bucket/
gsutil cp gs://carburants-composer-bucket/data.zip data.zip


# upload a DAG to the environment
gcloud composer environments storage dags import \
    --environment cli-1 \
    --location europe-west2 \
    --source dags/operators_dag.py

# list Dags
gcloud composer environments storage dags list \
    --environment cli-1 \
    --location europe-west2

