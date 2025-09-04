from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.hh_vacancies_utils import initial_load, incremental_load

with DAG(
    "hh_vacancies",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 * * *",  # каждый день в 06:00
    catchup=False,
    tags=["hh", "etl"],
) as dag:

    init_task = PythonOperator(
        task_id="initial_load",
        python_callable=initial_load
    )

    daily_task = PythonOperator(
        task_id="incremental_load",
        python_callable=incremental_load
    )

    init_task >> daily_task
