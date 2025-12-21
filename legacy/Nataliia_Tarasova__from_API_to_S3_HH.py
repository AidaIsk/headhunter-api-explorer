import io
import pandas as pd
from datetime import datetime, timezone
import pendulum
import logging
import requests

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

# импортируем класс-обработчик для загрузки данных в minio
from utils.hh_loader_s3 import HHVacanciesLoader

doc_md = """
# DAG: Ежедневная выгрузка вакансий HH в Minio

Данный DAG осуществляет **ежедневную** выгрузку вакансий с платформы **HeadHunter (hh.ru)** по ключевым словам и регионам России, и сохраняет результаты в **Minio (S3)**.  

---

## Основные функции DAG

- Выгрузка вакансий по заранее заданным ключевым словам.
- Поддержка регионов России без вложенных подразделов.
- Сохранение данных в формате **Parquet** в Minio.
- Отправка уведомлений после выполнения DAG:
  - В **Telegram**.

---

## Особенности работы

- Есть возможность **перегрузки ретро-данных**: можно запускать DAG за предыдущие даты.  
- Если в S3 уже есть файл за указанную дату, новые данные **заменяют старые**.
- Уведомления отправляются в любом случае, независимо от успешности выгрузки.
"""

default_args = {
    "owner": 'Nataliia_Tarasova',
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=3)
}

# Вспомогательные функции
def load_to_s3(**context):
    loader = HHVacanciesLoader()
    hook = S3Hook(aws_conn_id=loader.aws_conn_id)
    key = f"{loader.S3_PREFIX}/{context['ds']}.parquet"

    regions = loader.get_regions()
    all_data = []

    for region in regions:
        for kw in loader.KEYWORDS:
            bus_date = datetime.strptime(context['ds'], "%Y-%m-%d").date()
            df = loader.fetch_vacancies(region["id"], kw, bus_date)
            if df is not None:
                all_data.append(df)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        logging.info(f"Итого вакансий для загрузки: {len(final_df)}")
    else:
        final_df = pd.DataFrame()
        logging.warning("Нет данных для загрузки в S3")

    buffer = io.BytesIO()
    final_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    hook.load_file_obj(buffer, bucket_name=loader.S3_BUCKET, key=key, replace=True)
    logging.info(f"Загружено {len(final_df)} строк в s3://{loader.S3_BUCKET}/{key}")



# ---------------- Notifications ----------------
def send_telegram_message(**context):
    try:
        telegram_token = Variable.get('TG_BOT_TOKEN')
        chat_id = Variable.get('TG_BOT_CHAT_ID')

        dag_id = context['dag'].dag_id
        ti_load = context["dag_run"].get_task_instance("load_to_s3")
        
        load_state = ti_load.state
        # интересует только load_task

        if load_state == "success":
            status = "✅ *SUCCESS*"
            severity = "info"
        else:
            status = "❌ *FAILED*"
            severity = "critical"

        message = f"""
🔥 *Airflow Alert* 🔥

*DAG:* `{dag_id}`
*Task:* `load_to_s3`
*Status:* {status}

*SEVERITY:* {severity.upper()}
*Run ID:* {ti_load.run_id}

🕒 {ti_load.end_date.strftime('%Y-%m-%d %H:%M:%S') if ti_load.end_date else 'N/A'}
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
            logging.info(f"Telegram уведомление отправлено: {message}")

    except Exception as e:
        logging.error(f"Не удалось отправить сообщение в Telegram: {e}")




# ---------------- DAG ----------------
with DAG(
    dag_id="Nataliia_Tarasova__from_API_to_S3_HH",
    start_date=pendulum.datetime(2025, 11, 16, tz="UTC"),
    schedule_interval="0 7 * * *",
    catchup=True,
    tags=["hh"],
    default_args=default_args
) as dag:

    dag.doc_md = doc_md

    load_task = PythonOperator(
        task_id="load_to_s3",
        python_callable=load_to_s3,
        execution_timeout=pendulum.duration(minutes=15)
    )


    telegram_notify_task = PythonOperator(
        task_id='send_telegram_notification',
        python_callable=send_telegram_message,
        # запускается, есди даже load_task упал
        trigger_rule=TriggerRule.ALL_DONE
    )

    load_task >> telegram_notify_task



        





