from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_to_local_operator import GoogleCloudStorageToLocalFileOperator
import xml.etree.ElementTree as ET


def parse_xml_with_xcom(**context):
    ti = context['ti']
    xml_file_path = ti.xcom_pull(task_ids='unzip_file')
    parse_xml(xml_file_path)


def parse_xml():
    try:
        tree = ET.parse('data.xml')
        root = tree.getroot()
        print('=========== XML parsed successfully.')
        data = []
        for item in root.findall('row'):
            row = {}
            row['id']           = int(item.find('id').text)     if item.find('id').text is not None else None
            row['nom']          = item.find('nom').text         if item.find('nom').text is not None else None
            row['prenom']       = item.find('prenom').text      if item.find('prenom').text is not None else None
            data.append(row)
        print('=========== Data loaded successfully.')
        print(data)       
        # return data
    except Exception as e:
        print(e)




# === DAG & tasks =========================================================
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('gcs_dag', 
          default_args=default_args, 
          schedule_interval='*/30 * * * *',
          catchup=False
          )


# download_task = GoogleCloudStorageDownloadOperator(
#     task_id='download_file',
#     bucket='carburants-composer-bucket',
#     object='your-object-name',
#     filename='your-filename',
#     dag=dag,
# )

# unzip_task = BashOperator(
#     task_id='unzip_file',
#     bash_command='gsutil cp gs://carburants-composer-bucket/data.zip . && unzip data.zip && rm data.zip && gsutil cp data.xml gs://carburants-composer-bucket/data.xml',
#     dag=dag
# )

# parse_xml = PythonOperator(
#     task_id='parse_xml',
#     python_callable=parse_xml,
#     dag=dag,
# )

# bigquery_insert = PythonOperator(
#     task_id='bigquery_insert',
#     python_callable=bigquery_insert,
#     dag=dag,
# )


download_file = GoogleCloudStorageToLocalFileOperator(
    task_id='download_file',
    bucket='carburants-composer-bucket',
    object_name='data.zip',
    filename='data.xml',
    dag=dag
)

unzip_task = BashOperator(
    task_id='unzip_file',
    # bash_command='gsutil cp gs://carburants-composer-bucket/data.zip . && unzip data.zip && rm data.zip && gsutil cp data.xml gs://carburants-composer-bucket/data.xml && echo data.xml',
    bash_command='unzip data.zip && rm data.zip && echo data.xml',
    dag=dag,
    do_xcom_push=True
)

parse_xml = PythonOperator(
    task_id='parse_xml',
    python_callable=parse_xml_with_xcom,
    provide_context=True,
    dag=dag,
)


















# download_task >> unzip_task >> convert_task
download_file >> unzip_task >> parse_xml