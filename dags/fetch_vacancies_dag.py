from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import time

# Ключевые слова для поиска
KEYWORDS = [
    "data engineer",
    "инженер данных",
    "sql разработчик",
    "dba",
    "администратор баз данных",
    "etl",
    "bi analyst",
]

def fetch_vacancies():
    base_url = "https://api.hh.ru/vacancies"
    areas_url = "https://api.hh.ru/areas"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0 Safari/537.36"
        )
    }

    # Получаем регионы России
    response = requests.get(areas_url, headers=headers)
    response.raise_for_status()
    areas = response.json()

    russia = next(a for a in areas if a["name"] == "Россия")
    # ✅ Берём только регионы без под-регионов (например, Саратовская область, без городов)
    regions = [region for region in russia["areas"] if not region["areas"]]

    all_vacancies = []

    for keyword in KEYWORDS:
        for region in regions:
            region_id = region["id"]
            region_name = region["name"]

            for page in range(2):  # тест: только первые страницы
                params = {
                    "text": keyword,
                    "area": region_id,
                    "per_page": 20,
                    "page": page,
                }
                resp = requests.get(base_url, params=params, headers=headers)

                if resp.status_code == 403:
                    print(f"⚠️ Forbidden for {keyword} in {region_name}, skipping")
                    continue
                if resp.status_code == 400:
                    print(f"⚠️ Bad Request for {region_name} (id={region_id}), skipping")
                    continue

                resp.raise_for_status()
                data = resp.json()

                for v in data.get("items", []):
                    all_vacancies.append({
                        "region": region_name,
                        "keyword": keyword,
                        "id": v["id"],
                        "name": v["name"],
                        "employer": v["employer"]["name"] if v.get("employer") else None,
                        "published_at": v["published_at"],
                        "url": v["alternate_url"],
                        "salary_from": v["salary"]["from"] if v.get("salary") else None,
                        "salary_to": v["salary"]["to"] if v.get("salary") else None,
                        "currency": v["salary"]["currency"] if v.get("salary") else None,
                    })

                time.sleep(0.5)  # задержка между запросами

    # Сохраняем в CSV
    os.makedirs("/opt/airflow/dags/data", exist_ok=True)
    df = pd.DataFrame(all_vacancies)
    df.to_csv("/opt/airflow/dags/data/vacancies.csv", index=False)
    print(f"✅ Saved {len(df)} vacancies")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_vacancies_dag",
    default_args=default_args,
    description="Сбор вакансий HH.ru по регионам и ключевым словам (data engineer и смежные)",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["hh", "vacancies", "data"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_vacancies",
        python_callable=fetch_vacancies,
    )




