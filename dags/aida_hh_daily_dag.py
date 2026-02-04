from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.hh_ids import build_vacancies_ids_manifest
from utils.aida_hh_minio import pipeline_hh_to_bronze_json
import utils.natalia_hh_postgres as pg_utils

default_args = {
    "owner" : "aida",
    "retries": 1,
}


with DAG(
    dag_id="aida_hh_daily_json",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
    tags=["aida", "hh", "daily"],
) as dag:
    
    collect_bronze_json = PythonOperator(
        task_id="collect_hh_bronze_json",
        python_callable=pipeline_hh_to_bronze_json,
        op_kwargs={
            "ds": "{{ ds }}",
            "load_type": "daily"
        },
    )
    build_vacancies_ids = PythonOperator(
        task_id="build_vacancies_ids_manifest",
        python_callable=build_vacancies_ids_manifest,
        op_kwargs={
            "ds": "{{ ds }}",
            "load_type": "daily",
        },
    )
    trigger_details_dag = TriggerDagRunOperator(
        task_id="trigger_hh_details_dag",
        trigger_dag_id="aida_hh_details_daily",
        conf={
            "ds": "{{ ds }}", 
            "load_type": "daily",
        },
        reset_dag_run=True,
        wait_for_completion=False,
    )
    telegram_notify_task = PythonOperator(
        task_id='send_telegram_notification',
        python_callable=pg_utils.send_telegram_notification,
        op_kwargs={"watched_tasks": ["collect_hh_bronze_json", "build_vacancies_ids_manifest"]},
        trigger_rule=TriggerRule.ALL_DONE
    )

    collect_bronze_json >> build_vacancies_ids >> trigger_details_dag >> telegram_notify_task
