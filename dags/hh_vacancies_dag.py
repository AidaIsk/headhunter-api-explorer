import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- Константы ---
KEYWORDS = [
    "data engineer",
    "инженер данных",
    "sql разработчик",
    "dba",
    "администратор баз данных",
    "etl",
    "bi analyst",
]

AREAS_URL = "https://api.hh.ru/areas"
VACANCIES_URL = "https://api.hh.ru/vacancies"
HEADERS = {"User-Agent": "Mozilla/5.0"}

CSV_PATH = "/opt/airflow/dags/hh_vacancies.csv"


# --- Вспомогательные функции ---
def get_regions():
    """Получаем список регионов верхнего уровня в России."""
    resp = requests.get(AREAS_URL, headers=HEADERS)
    resp.raise_for_status()
    russia = next(a for a in resp.json() if a["name"] == "Россия")
    return [region for region in russia["areas"] if not region["areas"]]


def fetch_vacancies(region_id, keyword, start_date, end_date):
    """Выгрузка вакансий по региону и ключевому слову за период."""
    vacancies = []
    page = 0

    while True:
        params = {
            "text": keyword,
            "area": region_id,
            "per_page": 100,
            "page": page,
            "date_from": start_date.isoformat(),
            "date_to": end_date.isoformat(),
        }
        resp = requests.get(VACANCIES_URL, headers=HEADERS, params=params)
        if resp.status_code == 403:
            break
        resp.raise_for_status()
        data = resp.json()

        for v in data.get("items", []):
            vacancies.append({
                "id": v["id"],
                "name": v["name"],
                "region": v["area"]["name"],
                "keyword": keyword,
                "employer": v["employer"]["name"] if v.get("employer") else None,
                "published_at": v["published_at"],
                "url": v["alternate_url"],
                "salary_from": v["salary"]["from"] if v.get("salary") else None,
                "salary_to": v["salary"]["to"] if v.get("salary") else None,
                "currency": v["salary"]["currency"] if v.get("salary") else None,
            })

        if page >= data.get("pages", 1) - 1:
            break
        page += 1

    return vacancies


def save_to_csv(vacancies, mode="w"):
    """Сохраняем вакансии в CSV (append для инкремента)."""
    df = pd.DataFrame(vacancies)
    if not df.empty:
        df.to_csv(CSV_PATH, mode=mode, header=(mode == "w"), index=False)


# --- Основные функции для DAG ---
def initial_load():
    """Историческая выгрузка за последние 30 дней."""
    regions = get_regions()
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=30)
    all_vacancies = []

    for region in regions:
        for kw in KEYWORDS:
            all_vacancies.extend(fetch_vacancies(region["id"], kw, start_date, end_date))

    save_to_csv(all_vacancies, mode="w")


def incremental_load():
    """Догрузка вакансий за последние сутки."""
    regions = get_regions()
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=1)
    all_vacancies = []

    for region in regions:
        for kw in KEYWORDS:
            all_vacancies.extend(fetch_vacancies(region["id"], kw, start_date, end_date))

    save_to_csv(all_vacancies, mode="a")


# --- DAG ---
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
