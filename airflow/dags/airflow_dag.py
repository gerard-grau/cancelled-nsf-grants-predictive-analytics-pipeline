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

    # -------- COL·LECCIÓ (LANDING) --------
    collect_awards = BashOperator(
        task_id="collect_awards",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python collect_awards.py"
        ),
    )

    collect_terminated = BashOperator(
        task_id="collect_terminated",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python collect_terminated.py"
        ),
    )

    collect_cruz_list = BashOperator(
        task_id="collect_cruz_list",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python collect_cruz_list.py"
        ),
    )

    collect_legislators = BashOperator(
        task_id="collect_legislators",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python collect_legislators.py"
        ),
    )

    # -------- FORMATEIG → MONGO --------
    format_awards = BashOperator(
        task_id="format_awards",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python format_awards.py"
        ),
    )

    format_terminated = BashOperator(
        task_id="format_terminated",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python format_terminated.py"
        ),
    )

    format_cruz_list = BashOperator(
        task_id="format_cruz_list",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python format_cruz_list.py"
        ),
    )

    format_legislators = BashOperator(
        task_id="format_legislators",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python format_legislators.py"
        ),
    )

    # -------- TRANSFORMACIÓ MONGO → DELTA --------
    mongo_to_delta = BashOperator(
        task_id="mongo_to_delta",
        bash_command=(
            f"cd {PROJECT_SCRIPTS_DIR} && "
            "python data_transformer.py"
        ),
    )

    # Dependències:
    # 1) Tots els collect_* comencen en paral·lel
    # 2) Cada format_* depèn del seu collect_* corresponent
    # 3) mongo_to_delta espera a que tots els format_* hagin acabat

    collect_awards >> format_awards
    collect_terminated >> format_terminated
    collect_cruz_list >> format_cruz_list
    collect_legislators >> format_legislators

    [format_awards, format_terminated, format_cruz_list, format_legislators] >> mongo_to_delta
