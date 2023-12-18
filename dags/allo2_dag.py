from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import requests
import zipfile
import xml.etree.ElementTree as ET
import os
import json


bash_script = """
    #!/bin/bash

    url="https://donnees.roulez-eco.fr/opendata/instantane"
    file="file.zip"

    # Download the file
    curl -o $file $url
    if [ $? -eq 0 ]; then
        echo "===== File saved successfully ====="
    else
        echo "Error downloading file"
        exit 1
    fi

    # Unzip the file
    unzip -o $file -d ./
    if [ $? -eq 0 ]; then
        echo "===== File unzipped successfully ====="
    else
        echo "Error unzipping file"
        exit 1
    fi
"""

bash_script_ls = """
    #!/bin/bash

    ls -l
"""

def parse_xml():
    try:
        tree = ET.parse('PrixCarburants_instantane.xml')
        root = tree.getroot()
        print('XML parsed successfully.')

        dt_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('Loading ...')

        data = []
        for item in root.findall('pdv')[:3]:
            record = {
                'id': int(item.get('id')) if item.get('id') is not None else None,
                'record_timestamp': dt_now,
                'latitude': float(item.get('latitude')) if item.get('latitude') is not None else None,
                'longitude': float(item.get('longitude')) if item.get('longitude') is not None else None,
                'cp': item.get('cp') if item.get('cp') is not None else None,
                'pop': item.get('pop') if item.get('pop') is not None else None,
                'adresse': item.find('adresse').text if item.find('adresse') is not None else None,
                'ville': item.find('ville').text if item.find('ville') is not None else None,
                'horaires': item.get('horaires') if item.get('horaires') is not None else None,
                'services': item.get('services') if item.get('services') is not None else None,
                'gazole_maj': datetime.strptime(item.find("prix[@nom='Gazole']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='Gazole']") is not None else None,
                'gazole_prix': float(item.find("prix[@nom='Gazole']").get('valeur')) if item.find("prix[@nom='Gazole']") is not None else None,
                'sp95_maj': datetime.strptime(item.find("prix[@nom='SP95']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='SP95']") is not None else None,
                'sp95_prix': float(item.find("prix[@nom='SP95']").get('valeur')) if item.find("prix[@nom='SP95']") is not None else None,
                'e85_maj': datetime.strptime(item.find("prix[@nom='E85']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='E85']") is not None else None,
                'e85_prix': float(item.find("prix[@nom='E85']").get('valeur')) if item.find("prix[@nom='E85']") is not None else None,
                'gplc_maj': datetime.strptime(item.find("prix[@nom='GPLc']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='GPLc']") is not None else None,
                'gplc_prix': float(item.find("prix[@nom='GPLc']").get('valeur')) if item.find("prix[@nom='GPLc']") is not None else None,
                'e10_maj': datetime.strptime(item.find("prix[@nom='E10']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='E10']") is not None else None,
                'e10_prix': float(item.find("prix[@nom='E10']").get('valeur')) if item.find("prix[@nom='E10']") is not None else None,
                'sp98_maj': datetime.strptime(item.find("prix[@nom='SP98']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='SP98']") is not None else None,
                'sp98_prix': float(item.find("prix[@nom='SP98']").get('valeur')) if item.find("prix[@nom='SP98']") is not None else None
            }
            data.append(record)

        # save to json list file
        with open('data.json', 'w') as f:
            json.dump(data, f)
        print('json saved successfully.')    
        print(data)

    except Exception as e:
        print(e)

    try:
        query = """
            INSERT INTO `carburants-composer.test_dataset.test_carbu` (id, record_timestamp, latitude, longitude, cp, pop, adresse, ville, horaires, services, gazole_maj, gazole_prix, sp95_maj, sp95_prix, e85_maj, e85_prix, gplc_maj, gplc_prix, e10_maj, e10_prix, sp98_maj, sp98_prix)
            VALUES
            """

        for item in data:
            query += f"""(
                {item['id']},
                '{item['record_timestamp']}',
                {item['latitude']},
                {item['longitude']},
                '{item['cp']}',
                '{item['pop']}',
                '{item['adresse']}',
                '{item['ville']}',
                '{item['horaires']}',
                '{item['services']}',
                '{item['gazole_maj']}',
                {item['gazole_prix']},
                '{item['sp95_maj']}',
                {item['sp95_prix']},
                '{item['e85_maj']}',
                {item['e85_prix']},
                '{item['gplc_maj']}',
                {item['gplc_prix']},
                '{item['e10_maj']}',
                {item['e10_prix']},
                '{item['sp98_maj']}',
                {item['sp98_prix']}
            ),"""

        query = query[:-1]

        with open('query.sql', 'w') as f:
            f.write(query)

        print('query saved successfully.')
    except Exception as e:
        print(e)



default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "download_file_dag",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    description="DAG to download a file",
    schedule_interval='*/30 * * * *',
    catchup=False,
)

download_and_unzip = BashOperator(
    task_id='download_and_unzip',
    bash_command=bash_script,
    dag=dag,
)

list_files_task = BashOperator(
    task_id='list_files',
    bash_command=bash_script_ls,
    dag=dag,
)

parse_task = PythonOperator(
    task_id="parse_xml_task",
    python_callable=parse_xml,
    dag=dag,
)

# insert_query_job = BigQueryInsertJobOperator(
#             task_id="insert_query_job",
#             configuration={
#                 "query": {
#                     "query": "",
#                     "useLegacySql": False,
#                     "priority": "BATCH",
#                 }
#             },
#             location='europe-west2',
#         )





download_and_unzip >> list_files_task >> parse_task