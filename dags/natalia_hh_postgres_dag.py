from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime
import requests
import logging

import utils.natalia_hh_postgres as pg_utils


default_args = {
    "owner" : "nataliia",
    "retries": 1,
}

with DAG(
    dag_id='nataliia_hh_to_postgres',
    default_args=default_args,
    start_date=datetime(2025, 12, 25),
    schedule_interval='0 7 * * *',
    catchup=True,
    max_active_runs=1,
) as dag:

    # Создаем при необходимости таблицу в постгрес
    init_postgres_tables_task = PythonOperator(
        task_id='init_postgres_tables',
        python_callable=pg_utils.init_postgres_tables,
        op_kwargs={
            "postgres_conn_id": "postgres_bronze",
            "ddl_path": "dags/utils/ddl_pg_bronze.sql"
        }
    )

    # Проверяем новые файлы в MinIO
    check_new_files_task = PythonOperator(
        task_id='check_new_files',
        python_callable=pg_utils.check_new_files,
        op_kwargs={
            "bucket": "hh-raw",
            "prefix": "bronze/hh/vacancies_list/load_type=daily"
        }
    )

    # Загружаем новые файлы в Postgres
    load_to_postgres_task = PythonOperator(
        task_id='load_files_to_postgres',
        python_callable=pg_utils.load_to_postgres
    )

    telegram_notify_task = PythonOperator(
        task_id="send_telegram_notification",
        python_callable=pg_utils.send_telegram_notification,
        op_kwargs={"watched_tasks": ["load_files_to_postgres"]},
        trigger_rule=TriggerRule.ALL_DONE,  
)

    init_postgres_tables_task >> check_new_files_task >> load_to_postgres_task >> telegram_notify_task
