from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    print("Extracting data...")

def transform():
    print("Transforming data...")

def load():
    print("Loading data...")

with DAG(
    dag_id="sample_etl_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract
    )
    
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform
    )
    
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load
    )
    
    extract_task >> transform_task >> load_task