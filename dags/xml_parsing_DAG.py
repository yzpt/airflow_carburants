from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from xml_parsing import download_file, parse_xml, db_insert

start_date = datetime(2023, 11, 20, 12, 00)

default_args = {
    'owner': 'admin',
    'start_date': start_date,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

# with DAG('xml_parsing', default_args=default_args, schedule_interval='*/10 * * * *', catchup=False) as dag:
with DAG('xml_parsing', default_args=default_args, schedule_interval='* * * * *', catchup=False) as dag:

    download_file_task = PythonOperator(
        task_id='download_file',
        python_callable=download_file,
        dag=dag,
    )
    
    parse_xml_task = PythonOperator(
        task_id='parse_xml',
        python_callable=parse_xml,
        op_kwargs={'xml_file': 'instantanes_files/xml/PrixCarburants_instantane.xml'},
        dag=dag,
    )
    
    db_insert_task = PythonOperator(
        task_id='db_insert',
        python_callable=db_insert,
        op_kwargs={'root_string': parse_xml_task.output}, 
        dag=dag,
    )

    download_file_task >> parse_xml_task >> db_insert_task