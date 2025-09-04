import requests
import pandas as pd
import os
from datetime import datetime, timedelta, timezone

# ключевые слова для поиска близкихк инженеру данных профессий
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
# в заголовок передаем информацию о нас как о пользователе
HEADERS = {"User-Agent": "Mozilla/5.0"}

# по этому пути в контейнере airflow webserver будет помещен даг
CSV_PATH = "/opt/airflow/dags/data/hh_vacancies.csv"


# --- Вспомогательные функции ---
def get_regions():
    """Получаем список регионов верхнего уровня в России."""
    resp = requests.get(AREAS_URL, headers=HEADERS)
    resp.raise_for_status()
    # генераторное выражение для фильтрации регонов России; next берёт первый подходящий элемент из генератора.
    russia = next(a for a in resp.json() if a["name"] == "Россия")
    # получаем список тех регионов из российских регионов без вложенных областей 
    return [region for region in russia["areas"] if not region["areas"]]


def fetch_vacancies(region_id, keyword, start_date, end_date):
    """Выгрузка вакансий по региону и ключевому слову за период."""
    # список для хранения найденных вакансий
    vacancies = []
    # текущая страница на HH
    page = 0

    # создаем бесконечный цикл, который будет загружать страницы с данными, пока не переберет все страницы. Выйдем из цикла через break
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
        # выходим из цикла, если сервис отказал нам по причине отсутствия нужных прав
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
                "currency": v["salary"]["currency"] if v.get("salary") else None
            })

        # выходим из цикла, если страниц больше нет
        if page >= data.get("pages", 1) - 1:
            break
        page += 1

    return vacancies


'''def save_to_csv(vacancies, mode="w"):
    """Сохраняем вакансии в CSV (append для инкремента)."""
    df = pd.DataFrame(vacancies)
    if not df.empty:
        # пишем заголовок, если режим открытия файла - перезапись
        df.to_csv(CSV_PATH, mode=mode, header=(mode == "w"), index=False)'''

def save_to_csv(vacancies, mode="w"):
    """Сохраняем вакансии в CSV, при append добавляем только новые записи (по id+name)."""
    df = pd.DataFrame(vacancies)

    if df.empty:
        return

    # дата выгрузки
    df["update_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Приводим id к строке, чтобы избежать ошибок merge
    df["id"] = df["id"].astype(str)

    if mode == "a" and os.path.exists(CSV_PATH):
        # Загружаем исторические данные
        old_df = pd.read_csv(CSV_PATH, usecols=["id", "name"])
        old_df["id"] = old_df["id"].astype(str)

        # Оставляем только новые записи
        df_new = df.merge(old_df, on=["id", "name"], how="left", indicator=True)
        df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

        if df_new.empty:
            print("✅ Новых данных нет, файл не изменён")
            return

        # Дописываем новые строки
        df_new.to_csv(CSV_PATH, mode="a", header=False, index=False)
        print(f"➕ Добавлено {len(df_new)} новых строк")

    else:
        # Первый запуск или mode="w"
        df.to_csv(CSV_PATH, mode="w", header=True, index=False)
        print(f"💾 Сохранено {len(df)} строк (новый файл)")




# --- Основные функции для DAG ---
def initial_load():
    """Историческая выгрузка за последние 30 дней."""
    regions = get_regions()
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=30)
    all_vacancies = []

# ищем вакансии в каждом регионе по всем ключевым словам
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

# ищем вакансии в каждом регионе по всем ключевым словам
    for region in regions:
        for kw in KEYWORDS:
            all_vacancies.extend(fetch_vacancies(region["id"], kw, start_date, end_date))

    save_to_csv(all_vacancies, mode="a")