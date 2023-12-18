from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import requests
import zipfile
import xml.etree.ElementTree as ET
import os
import json
import tempfile

from google.cloud import storage


def download_and_unzip_file(**kwargs):
    try:
        COMPOSER_BUCKET = os.environ["COMPOSER_BUCKET"]

        url = "https://donnees.roulez-eco.fr/opendata/instantane"
        response = requests.get(url)
        zip_filename = "instantane.zip"

        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, zip_filename)

            with open(zip_path, 'wb') as file:
                file.write(response.content)
            print('File downloaded successfully.')

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            print('File unzipped successfully.')

            xml_file_path = os.path.join(temp_dir, "PrixCarburants_instantane.xml")

            # Upload to GCS
            client = storage.Client()
            bucket = client.get_bucket(os.environ["COMPOSER_BUCKET"])
            blob = bucket.blob("data/instantane.xml")
            blob.upload_from_filename(xml_file_path)
            print(f"File uploaded to: gs://{os.environ['COMPOSER_BUCKET']}/data/instantane.xml")
    
    except Exception as e:
        print(e)
        raise e


def convert_xml_to_json():
    try:
        client = storage.Client()
        bucket = client.get_bucket(os.environ['COMPOSER_BUCKET'])
        blob = bucket.blob("data/instantane.xml")
        blob.download_to_filename("PrixCarburants_instantane.xml")
        print(f"File downloaded from: gs://{os.environ['COMPOSER_BUCKET']}/data/instantane.xml")

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
                'ville': item.get('ville') if item.get('ville') is not None else None,
                'adresse': item.get('adresse') if item.get('adresse') is not None else None,
                'services': item.get('services') if item.get('services') is not None else None,
                'prix': [
                    {
                        'nom': price_item.get('nom') if price_item.get('nom') is not None else None,
                        'valeur': float(price_item.get('valeur')) if price_item.get('valeur') is not None else None,
                        'maj': price_item.get('maj') if price_item.get('maj') is not None else None,
                    }
                    for price_item in item.findall('prix')
                ],
            }
            data.append(record)
        print('Data loaded successfully.')

        # Save to JSON file
        with tempfile.TemporaryDirectory() as temp_dir:
            json_file_path = os.path.join(temp_dir, "data.json")
            with open(json_file_path, 'w') as f:
                json.dump(data, f)
            print('json saved successfully.')

            # Upload to GCS
            client = storage.Client()
            bucket = client.get_bucket(os.environ["COMPOSER_BUCKET"])
            blob = bucket.blob("data/data.json")
            blob.upload_from_filename(json_file_path)
            print(f"File uploaded to: gs://{os.environ['COMPOSER_BUCKET']}/data/data.json")

    except Exception as e:
        print(e)
        raise e

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

operators_dag = DAG(
    "operators_dag",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    description="operators ELT DAG",
    schedule_interval='*/30 * * * *',
    catchup=False,
)

download_and_unzip = PythonOperator(
    task_id='download_and_unzip',
    python_callable=download_and_unzip_file,
    provide_context=True,  # This is important to provide the context with **kwargs
    dag=operators_dag,
)




download_and_unzip