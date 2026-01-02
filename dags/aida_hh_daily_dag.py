from datetime import datetime
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from utils.hh_ids import build_vacancies_ids_manifest


from utils.aida_hh_minio import pipeline_hh_to_bronze_json

default_args = {
    "owner" : "aida",
    "retries": 1,
}

def send_telegram_message(**context):
    try:
        telegram_token = Variable.get('TG_BOT_TOKEN')
        chat_id = Variable.get('TG_BOT_CHAT_ID')

        dag = context['dag']
        dag_run = context['dag_run']

        # 👇 все таски, за которыми следим
        watched_tasks = [
            "collect_hh_bronze_json",
            "build_vacancies_ids_manifest",
        ]

        task_results = []
        failed = False

        for task_id in watched_tasks:
            ti = dag_run.get_task_instance(task_id)
            state = ti.state if ti else "unknown"

            if state != "success":
                failed = True

            icon = "✅" if state == "success" else "❌"
            task_results.append(f"{icon} `{task_id}` — *{state.upper()}*")

        overall_status = "❌ *FAILED*" if failed else "✅ *SUCCESS*"
        severity = "CRITICAL" if failed else "INFO"

        message = f"""
🔥 *Airflow Alert* 🔥

*DAG:* `{dag.dag_id}`
*Overall status:* {overall_status}
*Severity:* `{severity}`

*Tasks:*
{chr(10).join(task_results)}

*Run ID:* `{dag_run.run_id}`
🕒 {dag_run.end_date.strftime('%Y-%m-%d %H:%M:%S') if dag_run.end_date else 'N/A'}
        """

        url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        })

        if resp.status_code != 200:
            logging.error(f"Ошибка отправки Telegram: {resp.text}")
        else:
            logging.info("Telegram уведомление отправлено")

    except Exception as e:
        logging.error(f"Не удалось отправить сообщение в Telegram: {e}")


with DAG(
    dag_id="aida_hh_daily_json",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
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
    telegram_notify_task = PythonOperator(
        task_id='send_telegram_notification',
        python_callable=send_telegram_message,
        # запускается, есди даже load_task упал
        trigger_rule=TriggerRule.ALL_DONE
    )

    collect_bronze_json >> build_vacancies_ids >> telegram_notify_task