# Airflow & PostgreSQL : Prix des carburants en France

Implementing a data pipeline with Airflow and PostgreSQL through different approaches:
- Local with python environment
- Local cluster of containers with docker compose
- GCP : Composer & BigQuery

<hr>

- [Airflow \& PostgreSQL : Prix des carburants en France](#airflow--postgresql--prix-des-carburants-en-france)
      - [Data source](#data-source)
  - [1. Local implementation](#1-local-implementation)
    - [1.1. Python environment](#11-python-environment)
    - [1.2. Postgresql](#12-postgresql)
      - [Create user and database](#create-user-and-database)
      - [Create table](#create-table)
    - [1.3. Airflow](#13-airflow)
      - [Airflow initialization](#airflow-initialization)
      - [Star scheduler on a new terminal](#star-scheduler-on-a-new-terminal)
      - [Start webserver on a new terminal](#start-webserver-on-a-new-terminal)
      - [Airflow UI](#airflow-ui)
      - [Check the data](#check-the-data)
        - [Checking data : if peer authentication error](#checking-data--if-peer-authentication-error)
      - [Close the webserver](#close-the-webserver)
  - [2. Docker implementation](#2-docker-implementation)
    - [2.1. Postgresql](#21-postgresql)
    - [2.2. Airflow](#22-airflow)


<hr>

#### Data source
[https://www.prix-carburants.gouv.fr/rubrique/opendata/](https://www.prix-carburants.gouv.fr/rubrique/opendata/)

## 1. Local implementation

### 1.1. Python environment

```bash
python3 -m venv venv
source venv/bin/activate

pip install psycopg2-binary
pip install "apache-airflow[celery]==2.7.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.10.txt"

pip freeze > requirements.txt
```

### 1.2. Postgresql 

#### Create user and database

```bash
# variables
export DB_NAME=carburants_database
export TABLE_NAME=records
export USER_NAME=carburants_user
export PASSWORD=pp

# create user and database
sudo -i -u postgres psql <<EOF
CREATE DATABASE $DB_NAME;
CREATE USER $USER_NAME WITH PASSWORD '$PASSWORD';
ALTER ROLE $USER_NAME SET client_encoding TO 'utf8';
ALTER ROLE $USER_NAME SET default_transaction_isolation TO 'read committed';
ALTER ROLE $USER_NAME SET timezone TO 'Europe/Paris';
GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $USER_NAME;
EOF
```
#### Create table

```bash
# create table
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
```

### 1.3. Airflow

#### Airflow initialization

```bash
# init airflow
export AIRFLOW_HOME=$(pwd)
airflow db init
airflow users create --username admin --password admin --firstname Yohann --lastname Zapart --role Admin --email yohann@zapart.com

# airflow.cfg --> don't load example dags
sed -i 's/load_examples = True/load_examples = False/g' airflow.cfg
```

#### Star scheduler on a new terminal

```bash
# New terminal
cd <project_path>
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)
airflow scheduler
```

#### Start webserver on a new terminal

```bash
# New terminal
cd <project_path>
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)
airflow webserver --port 8080
```

#### Airflow UI

Accessible at [http://localhost:8080](http://localhost:8080)

![dag_screen](./img/dag_screen.png)

#### Check the data

```bash
psql -U $USER_NAME $DB_NAME <<EOF
SELECT id, record_timestamp, ville, adresse, latitude, longitude 
FROM $TABLE_NAME 
WHERE lower(ville) = 'lille'
AND record_timestamp = (SELECT MAX(record_timestamp) FROM $TABLE_NAME WHERE lower(ville) = 'lille')
ORDER BY record_timestamp DESC;
\q
EOF
```

![check data screen](./img/check_screen.png)

##### Checking data : if peer authentication error 

  If psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: FATAL:  Peer authentication failed for user "user_test"

  Then, edit the pg_hba.conf file and change the method column to password for the local connection:

  ```bash
  # pg_hba.conf
  
  # TYPE  DATABASE        USER            ADDRESS                 METHOD
  local   all             all                                     password
  ```

  Then, restart the postgresql service:

  ```bash
  sudo service postgresql restart
  ```

#### Close the webserver

After closing the webserver, the process is still running in the background. To kill it, we need to find the PID and kill it.

```bash
# Read the PID from the file generated on the root by airflow
PID=$(cat airflow-webserver.pid)

# Kill the process
kill -9 $PID
```

## 2. Docker implementation


---> see "docker" branch



### 2.1. Postgresql

...

### 2.2. Airflow

[https://airflow.apache.org/docs/docker-stack/index.html](https://airflow.apache.org/docs/docker-stack/index.html)

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.3/docker-compose.yaml'
mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker compose up airflow-init
docker compose down --volumes --remove-orphans
docker compose up
```

...


## 3. GCP : Composer & BigQuery

```bash
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


source venv/bin/activate
pip install google-cloud-bigquery
pip install google-cloud-storage

# > dags/etl.py

# upload a DAG to the environment
gcloud composer environments storage dags import \
    --environment cli-1 \
    --location europe-west2 \
    --source dags/composer_dag.py

# venv-composer-dag
python3 -m venv venv-composer-dag
source venv-composer-dag/bin/activate
pip install requests
pip install lxml
pip freeze > requirements-composer-dag.txt

# add packages to the environment
gcloud composer environments update cli-1 \
    --location europe-west2 \
    --update-pypi-packages-from-file requirements-composer-dag.txt


# open the Airflow UI
gcloud composer environments describe cli-1 --location europe-west2 --format="value(config.airflowUri)"
```