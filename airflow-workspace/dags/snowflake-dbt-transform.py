"""
=============================================================================
DAG: dbt_finance_analytics
=============================================================================
PURPOSE:
    - Run dbt finance analytics project
    - Capture logs and upload to GCS
=============================================================================
"""

from datetime import datetime, timedelta
from pathlib import Path
import json
import shutil

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from cosmos import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode

# ============================================================================
# CONFIG
# ============================================================================

DBT_PROJECT_PATH = '/dbt-workspace/dbt-projects/finance_analytics'
GCS_BUCKET = 'finance-data-landing-bucket-poc'

# ============================================================================
# HELPER FUNCTION
# ============================================================================

def capture_logs(**context):
    """Capture dbt logs and prepare for upload"""
    
    execution_date = context['ds']
    run_id = context['dag_run'].run_id
    
    # Source paths
    log_dir = f"{DBT_PROJECT_PATH}/logs"
    target_dir = f"{DBT_PROJECT_PATH}/target"
    
    # Destination
    dest = f"/tmp/dbt_logs/{execution_date}/{run_id}"
    Path(dest).mkdir(parents=True, exist_ok=True)
    
    # Copy logs
    files_to_copy = {
        f"{log_dir}/dbt.log": f"{dest}/dbt.log",
        f"{target_dir}/run_results.json": f"{dest}/run_results.json",
        f"{target_dir}/manifest.json": f"{dest}/manifest.json",
    }
    
    for src, dst in files_to_copy.items():
        if Path(src).exists():
            shutil.copy2(src, dst)
            print(f"✅ Copied {Path(src).name}")
    
    # Create metadata
    metadata = {
        'execution_date': execution_date,
        'run_id': run_id,
        'timestamp': datetime.now().isoformat()
    }
    
    with open(f"{dest}/metadata.json", 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"📂 Logs ready at: {dest}")
    return dest

# ============================================================================
# DAG
# ============================================================================

default_args = {
    'owner': 'data_engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='snowflake-dbt-transform',
    default_args=default_args,
    description='Run dbt and store logs in GCS',
    schedule_interval='0 2 * * *',  # Daily 2 AM
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['dbt', 'finance'],
) as dag:

    # Run dbt
    dbt_run = DbtTaskGroup(
        group_id='dbt_run',
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=ProfileConfig(
            profile_name='finance_analytics',
            target_name='prod',
            profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path='/usr/local/bin/dbt',
        ),
        render_config=RenderConfig(
            select=['path:models'],
        ),
    )

    # Capture logs
    capture = PythonOperator(
        task_id='capture_logs',
        python_callable=capture_logs,
    )

    # Upload to GCS
    upload = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/tmp/dbt_logs/{{ ds }}/{{ dag_run.run_id }}/*',
        dst='dbt-logs/{{ ds }}/{{ dag_run.run_id }}/',
        bucket=GCS_BUCKET,
        gcp_conn_id='google_cloud_default',
    )

    # Dependencies
    dbt_run >> capture >> upload