from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from data_collection import main as collect_main
from data_formatter import main as format_main
from data_transformer import main as transform_main

default_args = {
    "owner": "eloi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nsf_awards_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval="0 3 * * *",  # cada dia a les 03:00
    catchup=False,
) as dag:

    collect_task = PythonOperator(
        task_id="collect_data",
        python_callable=collect_main,
    )

    format_task = PythonOperator(
        task_id="format_to_mongo",
        python_callable=format_main,
    )

    transform_task = PythonOperator(
        task_id="mongo_to_delta",
        python_callable=transform_main,
    )

    # dependències
    collect_task >> format_task >> transform_task
