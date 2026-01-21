from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime
from pathlib import Path

import utils.natalia_hh_postgres as pg_utils


default_args = {
    "owner" : "nataliia",
    "retries": 1,
}

with DAG(
    dag_id='nataliia_hh_details_to_postgres',
    default_args=default_args,
    start_date=datetime(2026, 1, 4),
    schedule_interval='0 3 * * *',
    catchup=True,
    max_active_runs=1,
) as dag:

    BASE_DIR = Path(__file__).resolve().parent
    SQL_DIR = BASE_DIR / "utils" / "sql"

    # Создаем при необходимости таблицу в постгрес
    init_postgres_tables_task = PythonOperator(
        task_id='init_postgres_tables',
        python_callable=pg_utils.init_postgres_tables,
        op_kwargs={
            "postgres_conn_id": "postgres_bronze",
            "ddl_path": str(SQL_DIR / "ddl_pg_bronze_details.sql")
        }
    )

    # Проверяем новые файлы в MinIO
    check_new_files_task = PythonOperator(
        task_id='check_new_files_batch',
        python_callable=pg_utils.check_new_files_batch,
        op_kwargs={
            "bucket": "hh-raw",
            "prefix": "bronze/hh/vacancy_details/load_type=daily"
        }
    )

    # Загружаем новые файлы в Postgres
    load_to_postgres_task = PythonOperator(
        task_id='load_files_to_postgres',
        python_callable=pg_utils.load_to_postgres_batch,
        op_kwargs={
            "bucket": "hh-raw",
            "prefix": "bronze/hh/vacancy_details/load_type=daily"
        }
    )

    # Загружаем новые файлы с отчетами в Postgres
    load_to_postgres_report_task = PythonOperator(
        task_id='load_files_to_postgres_report',
        python_callable=pg_utils.load_to_postgres_report,
        op_kwargs={
        "bucket": "hh-raw",
        "prefix": "bronze/hh/vacancy_details_coverage/load_type=daily"
        }
    )

    telegram_notify_task = PythonOperator(
        task_id="send_telegram_notification",
        python_callable=pg_utils.send_telegram_notification,
        op_kwargs={"watched_tasks": ["load_files_to_postgres"]},
        trigger_rule=TriggerRule.ALL_DONE,  
    )

    init_postgres_tables_task >> check_new_files_task >> load_to_postgres_task >> load_to_postgres_report_task>> telegram_notify_task