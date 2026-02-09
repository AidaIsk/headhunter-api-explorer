from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime
from pathlib import Path

from utils.natalia_hh_postgres import init_postgres_tables, send_telegram_notification

with DAG(
    dag_id="natalia_hh_gold_layer",
    start_date=datetime(2025, 12, 25),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    

    BASE_DIR = Path(__file__).resolve().parent
    SQL_DIR = BASE_DIR / "utils" / "sql"

    # init_postgres_tables_task = PythonOperator(
    #         task_id="init_gold_vacancy_risk_signals",
    #         python_callable=init_postgres_tables,
    #         op_kwargs={
    #             "postgres_conn_id": "postgres_bronze",
    #             "ddl_path": str(SQL_DIR / "ddl_pg_gold_vacancy_risk_signals.sql")
    #         }
    #     )

    refresh_gold_mv = PostgresOperator(
            task_id="refresh_gold_vacancy_risk_signals",
            postgres_conn_id="postgres_bronze",
            sql="REFRESH MATERIALIZED VIEW gold.vacancy_risk_signals;"
            )

    
    telegram_notify_task = PythonOperator(
        task_id="send_telegram_notification",
        python_callable=send_telegram_notification,
        op_kwargs={"watched_tasks": ["refresh_gold_vacancy_risk_signals"]},
        trigger_rule=TriggerRule.ALL_DONE,  
    )

    #init_postgres_tables_task  
    refresh_gold_mv >> telegram_notify_task