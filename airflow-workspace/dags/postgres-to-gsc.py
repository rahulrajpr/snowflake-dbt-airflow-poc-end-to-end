from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# ============================================
# CONFIG
# ============================================

SQL_ROOT = "/trino-sql"
SCHEMA_SQL = f"{SQL_ROOT}/schema/create_schema.sql"
TABLES_BASE_PATH = f"{SQL_ROOT}/tables"

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ============================================
# HELPERS
# ============================================

def discover_tables():
    tables = []

    for folder in os.listdir(TABLES_BASE_PATH):

        full_path = os.path.join(TABLES_BASE_PATH, folder)

        if (
            os.path.isdir(full_path)
            and os.path.exists(os.path.join(full_path, "create_table.sql"))
            and os.path.exists(os.path.join(full_path, "incremental.sql"))
        ):
            tables.append(folder)

    return tables


def read_sql(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Missing SQL file: {filepath}")

    with open(filepath, "r") as f:
        return f.read()

# ============================================
# DAG
# ============================================

with DAG(
    dag_id="postgres-to-gsc",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    # ----------------------------------------
    # CREATE HIVE SCHEMA
    # ----------------------------------------

    create_schema_if_not_exists = TrinoOperator(
        task_id="create_hive_schema",
        trino_conn_id="trino_default",
        sql=read_sql(SCHEMA_SQL),
    )

    # ----------------------------------------
    # DISCOVER TABLES
    # ----------------------------------------

    tables = discover_tables()

    for table in tables:

        create_sql = f"{TABLES_BASE_PATH}/{table}/create_table.sql"
        incremental_sql = f"{TABLES_BASE_PATH}/{table}/incremental.sql"

        create_table_if_not_exists = TrinoOperator(
            task_id=f"create_{table}_table",
            trino_conn_id="trino_default",
            sql=read_sql(create_sql),
        )

        incremental_load = TrinoOperator(
            task_id=f"incremental_{table}",
            trino_conn_id="trino_default",
            sql=read_sql(incremental_sql),
        )

        create_schema_if_not_exists >> create_table_if_not_exists >> incremental_load