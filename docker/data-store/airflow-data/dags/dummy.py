# File: data-store/airflow-data/dags/simple_dummy_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'simple_dummy_workflow',
    default_args=default_args,
    description='A simple workflow using dummy operators',
    schedule_interval='0 9 * * *',  # Runs daily at 9 AM
    catchup=False,  # Don't run old instances
    tags=['simple', 'test', 'learning'],
) as dag:
    
    # Define tasks using DummyOperator
    start = DummyOperator(
        task_id='start_pipeline',
    )
    
    extract_data = DummyOperator(
        task_id='extract_data',
    )
    
    transform_data = DummyOperator(
        task_id='transform_data',
    )
    
    load_data = DummyOperator(
        task_id='load_data',
    )
    
    validate_data = DummyOperator(
        task_id='validate_data',
    )
    
    send_notification = DummyOperator(
        task_id='send_notification',
    )
    
    end = DummyOperator(
        task_id='end_pipeline',
    )
    
    # Define the task dependencies (workflow)
    start >> extract_data >> transform_data >> load_data >> validate_data >> send_notification >> end