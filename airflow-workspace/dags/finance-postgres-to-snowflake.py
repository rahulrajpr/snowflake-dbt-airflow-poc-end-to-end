"""
Reusable Meltano ELT DAG Template
Executes Meltano sync jobs from Airflow with proper logging and error handling

This is a base template for future DAGs - just modify the configuration section
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# =================================================================
# CONFIGURATION - MODIFY THIS SECTION FOR EACH DAG
# =================================================================

# DAG Metadata
DAG_ID = 'meltano_postgres_to_snowflake'
DAG_DESCRIPTION = 'Sync PostgreSQL tables to Snowflake via Meltano with GCS staging'
DAG_OWNER = 'data_engineering'
DAG_TAGS = ['meltano', 'postgres', 'snowflake', 'elt']

# Schedule
SCHEDULE_INTERVAL = '0 2 * * *'  # 2 AM daily (cron format)

# Meltano Configuration
MELTANO_CONTAINER_NAME = 'meltano-container'
MELTANO_PROJECT_PATH = '/project'
EXTRACTOR_NAME = 'tap-postgres'
LOADER_NAME = 'target-snowflake'

# Source Configuration (for logging)
SOURCE_DATABASE = 'finance_db'
SOURCE_SCHEMA = 'finance_schema'
SOURCE_TABLES = ['funds', 'transactions', 'fund_holdings']  # For display only

# Target Configuration (for logging)
TARGET_DATABASE = 'FINANCE_DB'
TARGET_SCHEMA = 'POSTGRES_RAW'
TARGET_WAREHOUSE = 'POC_INJESTION_WAREHOUSE'

# GCS Staging Configuration (for logging)
GCS_BUCKET = 'finance-data-landing-bucket-poc'
GCS_FOLDER = 'meltano-staging'

# Error Handling
RETRIES = 2
RETRY_DELAY_MINUTES = 5

# =================================================================
# DAG DEFINITION - DO NOT MODIFY (unless changing logic)
# =================================================================

default_args = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': RETRIES,
    'retry_delay': timedelta(minutes=RETRY_DELAY_MINUTES),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=DAG_DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=DAG_TAGS,
) as dag:

    # =================================================================
    # TASK 1: Pre-Flight Checks
    # =================================================================
    
    pre_flight_checks = BashOperator(
        task_id='pre_flight_checks',
        bash_command=f"""
            set -e
            
            log_info() {{
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $1"
            }}
            
            log_error() {{
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $1" >&2
            }}
            
            log_info "=========================================="
            log_info "PRE-FLIGHT CHECKS"
            log_info "=========================================="
            
            # Check 1: Meltano container running
            log_info "Check 1: Meltano container status"
            if ! docker ps | grep -q "{MELTANO_CONTAINER_NAME}"; then
                log_error "Meltano container '{MELTANO_CONTAINER_NAME}' is not running"
                log_error "Start it with: docker-compose up -d meltano"
                exit 1
            fi
            log_info "✓ Meltano container is running"
            
            # Check 2: Meltano accessible
            log_info "Check 2: Meltano accessibility"
            if ! docker exec {MELTANO_CONTAINER_NAME} bash -c "cd {MELTANO_PROJECT_PATH} && meltano --version" >/dev/null 2>&1; then
                log_error "Cannot execute Meltano commands"
                exit 1
            fi
            log_info "✓ Meltano is accessible"
            
            # Check 3: Project configuration
            log_info "Check 3: Meltano project configuration"
            if ! docker exec {MELTANO_CONTAINER_NAME} bash -c "cd {MELTANO_PROJECT_PATH} && [ -f meltano.yml ]"; then
                log_error "meltano.yml not found in {MELTANO_PROJECT_PATH}"
                exit 1
            fi
            log_info "✓ meltano.yml found"
            
            # Check 4: Extractor connectivity
            log_info "Check 4: Source connectivity ({EXTRACTOR_NAME})"
            if ! docker exec {MELTANO_CONTAINER_NAME} bash -c "cd {MELTANO_PROJECT_PATH} && meltano invoke {EXTRACTOR_NAME} --discover" >/dev/null 2>&1; then
                log_error "Cannot connect to source via {EXTRACTOR_NAME}"
                exit 1
            fi
            log_info "✓ Source connection successful"
            
            log_info "=========================================="
            log_info "✓ ALL PRE-FLIGHT CHECKS PASSED"
            log_info "=========================================="
        """,
    )

    # =================================================================
    # TASK 2: Execute Meltano Sync
    # =================================================================
    
    execute_meltano_sync = BashOperator(
        task_id='execute_meltano_sync',
        bash_command=f"""
            set -e
            
            log_info() {{
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $1"
            }}
            
            log_error() {{
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $1" >&2
            }}
            
            START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
            
            log_info "=========================================="
            log_info "MELTANO SYNC EXECUTION"
            log_info "=========================================="
            log_info "Start Time: $START_TIME"
            log_info ""
            log_info "Pipeline Configuration:"
            log_info "  Extractor: {EXTRACTOR_NAME}"
            log_info "  Loader: {LOADER_NAME}"
            log_info ""
            log_info "Source:"
            log_info "  Database: {SOURCE_DATABASE}"
            log_info "  Schema: {SOURCE_SCHEMA}"
            log_info "  Tables: {', '.join(SOURCE_TABLES)}"
            log_info ""
            log_info "Target:"
            log_info "  Database: {TARGET_DATABASE}"
            log_info "  Schema: {TARGET_SCHEMA}"
            log_info "  Warehouse: {TARGET_WAREHOUSE}"
            log_info ""
            log_info "Staging:"
            log_info "  Bucket: gs://{GCS_BUCKET}/{GCS_FOLDER}/"
            log_info ""
            log_info "=========================================="
            log_info "Executing sync..."
            log_info "=========================================="
            log_info ""
            
            # Execute Meltano run command
            if docker exec {MELTANO_CONTAINER_NAME} bash -c "
                cd {MELTANO_PROJECT_PATH} &&
                meltano run {EXTRACTOR_NAME} {LOADER_NAME}
            "; then
                EXIT_CODE=0
            else
                EXIT_CODE=$?
            fi
            
            END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
            
            log_info ""
            log_info "=========================================="
            if [ $EXIT_CODE -eq 0 ]; then
                log_info "✓ SYNC COMPLETED SUCCESSFULLY"
                log_info "=========================================="
                log_info "Start Time: $START_TIME"
                log_info "End Time: $END_TIME"
                log_info ""
                log_info "Next Steps:"
                log_info "  1. Verify data in Snowflake:"
                log_info "     USE DATABASE {TARGET_DATABASE};"
                log_info "     USE SCHEMA {TARGET_SCHEMA};"
                log_info "     SELECT COUNT(*) FROM FUNDS;"
                log_info "  2. Check GCS staging:"
                log_info "     gsutil ls gs://{GCS_BUCKET}/{GCS_FOLDER}/"
            else
                log_error "✗ SYNC FAILED"
                log_error "=========================================="
                log_error "Start Time: $START_TIME"
                log_error "End Time: $END_TIME"
                log_error "Exit Code: $EXIT_CODE"
                log_error ""
                log_error "Troubleshooting:"
                log_error "  1. Check Meltano logs:"
                log_error "     docker logs {MELTANO_CONTAINER_NAME} --tail 100"
                log_error "  2. Check environment variables:"
                log_error "     docker exec {MELTANO_CONTAINER_NAME} env | grep MELTANO"
                log_error "  3. Verify Snowflake connection"
                log_error "  4. Check GCS permissions"
            fi
            
            exit $EXIT_CODE
        """,
    )

    # =================================================================
    # TASK 3: Log Final Status
    # =================================================================
    
    def log_final_status(**context):
        """Log final status and metrics"""
        ti = context['ti']
        dag_run = context['dag_run']
        
        logging.info("=" * 70)
        logging.info("PIPELINE EXECUTION SUMMARY")
        logging.info("=" * 70)
        logging.info(f"DAG ID: {context['dag'].dag_id}")
        logging.info(f"Run ID: {dag_run.run_id}")
        logging.info(f"Execution Date: {context['execution_date']}")
        logging.info(f"Logical Date: {context['logical_date']}")
        logging.info("")
        logging.info("Configuration:")
        logging.info(f"  Extractor: {EXTRACTOR_NAME}")
        logging.info(f"  Loader: {LOADER_NAME}")
        logging.info(f"  Source: {SOURCE_DATABASE}.{SOURCE_SCHEMA}")
        logging.info(f"  Target: {TARGET_DATABASE}.{TARGET_SCHEMA}")
        logging.info(f"  Tables: {', '.join(SOURCE_TABLES)}")
        logging.info("")
        logging.info("Status: SUCCESS")
        logging.info("=" * 70)
        
        return "Pipeline completed successfully"
    
    log_status = PythonOperator(
        task_id='log_final_status',
        python_callable=log_final_status,
        provide_context=True,
    )

    # =================================================================
    # TASK DEPENDENCIES
    # =================================================================
    
    pre_flight_checks >> execute_meltano_sync >> log_status