from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from utils.hh_details import build_details_coverage_report
from utils.hh_details import collect_vacancy_details
from utils.hh_ids import guard_has_ids
import utils.natalia_hh_postgres as pg_utils

default_args = {
    "owner" : "aida",
    "retries": 1,
}


with DAG(
    dag_id="aida_hh_details_daily",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False,
    max_active_runs = 1,
    tags=["aida", "hh_details", "daily"],
) as dag:
    
    guard_has_ids_task = PythonOperator(
        task_id="guard_has_ids",
        python_callable=guard_has_ids,
        op_kwargs={
            "ds": "{{ ds }}",
            "load_type": "daily",
        },
    )
    collect_vacancy_details_task = PythonOperator(
        task_id="collect_vacancy_details",
        python_callable=collect_vacancy_details,
        op_kwargs={
            "ds": "{{ ds }}",
            "load_type": "daily",
            "batch_size": 200,
        },
    )
    build_details_coverage_report_task = PythonOperator(
        task_id="build_details_coverage_report",
        python_callable=build_details_coverage_report,
        op_kwargs={
            "ds": "{{ ds }}",
            "load_type": "daily"
        },
    )
    telegram_notify_task = PythonOperator(
        task_id='send_telegram_notification',
        python_callable=pg_utils.send_telegram_notification,
        op_kwargs={"watched_tasks": ["collect_vacancy_details", "build_details_coverage_report"]},
        trigger_rule=TriggerRule.ALL_DONE
    )
    

guard_has_ids_task >> collect_vacancy_details_task
collect_vacancy_details_task >> build_details_coverage_report_task >> telegram_notify_task
