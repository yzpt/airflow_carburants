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
export AIRFLOW_HOME=/home/yzpt/projects/carburant_gcp
airflow scheduler

# starting webserver
source venv/bin/activate
export AIRFLOW_HOME=/home/yzpt/projects/carburant_gcp
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

# === Offical Airflow Docker ===========================================================================================
# https://airflow.apache.org/docs/docker-stack/index.html
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.3/docker-compose.yaml'
mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker compose up airflow-init
docker compose down --volumes --remove-orphans
docker compose up



# === try vendredi 24/11 ===========================================================================================
source venv/bin/activate
pip freeze > requirements.txt

sudo chmod 777 ./script/entrypoint.sh
# sudo chmod 777 ./requirements.txt

# > docker-compose.yaml
docker compose down
docker compose up airflow-postgres -d --remove-orphans
docker compose up -d airflow-postgres airflow-scheduler --remove-orphans
docker compose up -d airflow-postgres airflow-scheduler postgres-db --remove-orphans
# Error response from daemon: Ports are not available: exposing port TCP 0.0.0.0:5432 -> 0.0.0.0:0: listen tcp 0.0.0.0:5432: bind: address already in use

# airflow-postgres  ports : 5433:5432
# postgres-db       ports : 5434:5432

# > nb_psql_docker_service.ipynb
# connexion & création table depuis psycopg2 ok


# > dags/xml_parsing.py
# conn = psycopg2.connect(
#             database="psql_db_name",
#             user="postgres",
#             password="postgres",
#             host="localhost",
#             port="5434"
#         )
# > trigger dag

# airflow-scheduler
# *** !!!! Please make sure that all your Airflow components (e.g. schedulers, webservers, workers and triggerer) have the same 'secret_key' configured in 'webserver' section and time is synchronized on all your machines (for example with ntpd)
# See more at https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#secret-key
# *** Could not read served logs: Client error '403 FORBIDDEN' for url 'http://airflow-scheduler:8793/log/dag_id=xml_parsing/run_id=manual__2023-11-24T12:34:41.398552+00:00/task_id=download_file/attempt=1.log'
# For more information check: https://httpstatuses.com/403


# === ntpd :
# > sh_ntpd.sh