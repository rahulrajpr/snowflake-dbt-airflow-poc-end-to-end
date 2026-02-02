"""
Simple ELT Pipeline: Postgres → Snowflake
Just run Meltano, log everything, no fluff
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2026, 2, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'postgres_to_snowflake',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
    tags=['meltano', 'etl'],
) as dag:

    # Sync orders (incremental)
    sync_orders = BashOperator(
        task_id='sync_orders',
        bash_command="""
            echo "====== ORDERS SYNC START ======"
            echo "Time: $(date)"
            
            docker exec meltano-container meltano run tap-postgres target-snowflake --select tap-postgres.public-orders
            
            echo "====== ORDERS SYNC END ======"
            echo "Time: $(date)"
        """,
    )
    
    # Sync products (full table)
    sync_products = BashOperator(
        task_id='sync_products',
        bash_command="""
            echo "====== PRODUCTS SYNC START ======"
            echo "Time: $(date)"
            
            docker exec meltano-container meltano run tap-postgres target-snowflake --select tap-postgres.public-products
            
            echo "====== PRODUCTS SYNC END ======"
            echo "Time: $(date)"
        """,
    )
    
    sync_orders >> sync_products