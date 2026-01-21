from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime

from utils.silver_config import SILVER_TABLES
from utils.natalia_hh_pg_silver import load_silver_vacancies, load_silver_employers, load_silver_vacancy_skills, load_silver_vacancy_text
from utils.natalia_hh_postgres import init_postgres_tables, send_telegram_notification


with DAG(
    dag_id="natalia_hh_silver_layer",
    start_date=datetime(2025, 12, 1),
    schedule="0 4 * * *",
    catchup=True,
    max_active_runs=1,
) as dag:

    silver_load_task_ids = []
    silver_load_tasks = []

    for table in SILVER_TABLES:
        table_name = table["name"]
        load_task_id = f"load_to_pg_silver_{table_name}"
        silver_load_task_ids.append(load_task_id)


        init_postgres_tables_task = PythonOperator(
            task_id=f"init_silver_{table_name}",
            python_callable=init_postgres_tables,
            op_kwargs={
                "postgres_conn_id": "postgres_bronze",
                "ddl_path": f"dags/utils/sql/{table['ddl']}"
            }
        )

        load_to_postgres_task = PythonOperator(
            task_id=load_task_id,
            python_callable=table['load_func'],
            op_kwargs={
                "load_dt": "{{ execution_date }}"
            }
        )

        init_postgres_tables_task >> load_to_postgres_task
        silver_load_tasks.append(load_to_postgres_task)

    telegram_notify_task = PythonOperator(
        task_id="send_telegram_notification",
        python_callable=send_telegram_notification,
        op_kwargs={"watched_tasks": silver_load_task_ids},
        trigger_rule=TriggerRule.ALL_DONE,  
    )

    silver_load_tasks >> telegram_notify_task
