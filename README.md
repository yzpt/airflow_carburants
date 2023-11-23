# Airflow & PostgreSQL : Prix des carburants en France

Implementing a data pipeline with Airflow and PostgreSQL through three different approaches:
- Local
- Docker
- GCP : Composer & BigQuery

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

```bash
sudo apt install postgresql

# variables
DB_NAME=carburants
TABLE_NAME=records
USERNAME=user
PASSWORD=password

# create user and database
sudo -i -u postgres psql  <<EOF
CREATE DATABASE $DB_NAME;
CREATE USER $USERNAME WITH PASSWORD '$PASSWORD';
ALTER ROLE $USERNAME SET client_encoding TO 'utf8';
ALTER ROLE $USERNAME SET default_transaction_isolation TO 'read committed';
ALTER ROLE $USERNAME SET timezone TO 'Europe/Paris';
GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $USERNAME;
EOF

# create table
sudo -i -u $USERNAME psql $DB_NAME <<EOF
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

```bash
# init airflow
export AIRFLOW_HOME=/home/yzpt/projects/carburant_gcp
airflow db init
airflow users create --username admin --firstname Yohann --lastname Zapart --role Admin --email yohann@zapart.com

# airflow.cfg --> don't load example dags
sed -i 's/load_examples = True/load_examples = False/g' airflow.cfg
```
* New terminal : starting scheduler

    ```bash
    # starting scheduler
    source venv/bin/activate
    export AIRFLOW_HOME=/home/yzpt/projects/carburant_gcp
    airflow scheduler
    ```

* New terminal : starting webserver

    ```bash
    source venv/bin/activate
    export AIRFLOW_HOME=/home/yzpt/projects/carburant_gcp
    airflow webserver --port 8080
    ```

* Airflow UI accessible at [http://localhost:8080](http://localhost:8080)

    ![dag_screen](./img/dag_screen.png)

* Check the data : 

    ```bash
    # (psql_select.sh)

    USER_NAME=yzpt
    DB_NAME=carburants
    TABLE_NAME=records
    psql -U $USER_NAME $DB_NAME <<EOF
    SELECT id, record_timestamp, ville, adresse, latitude, longitude 
    FROM $TABLE_NAME 
    WHERE lower(ville) = 'lille'
    AND record_timestamp = (SELECT MAX(record_timestamp) FROM records WHERE lower(ville) = 'lille')
    ORDER BY record_timestamp DESC;
    \q
    EOF
    ```

    ![check data screen](./img/check_screen.png)

* Close the webserver

    After closing the webserver, the process is still running in the background. To kill it, we need to find the PID and kill it.

    ```bash
    # Read the PID from the file
    PID=$(cat airflow-webserver.pid)
    
    # Kill the process
    kill -9 $PID
    ```

## 2. Docker implementation

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

