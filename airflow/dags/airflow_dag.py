from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from pathlib import Path
# 🔴 CANVIA AQUEST PATH SI CAL
PROJECT_SCRIPTS_DIR = Path(__file__).parent.parent.parent / "scripts"
default_args = {
    "owner": "eloi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nsf_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 8),
    schedule_interval="@daily",   # execució diària
    catchup=False,
    tags=["nsf", "etl"],
) as dag:

    collect_data = BashOperator(
        task_id="collect_data",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python data_collection.py"
        ),
    )

    format_to_mongo = BashOperator(
        task_id="format_to_mongo",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python data_formatter.py"
        ),
    )

    mongo_to_delta = BashOperator(
        task_id="mongo_to_delta",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python data_transformer.py"
        ),
    )

    collect_data >> format_to_mongo >> mongo_to_delta
