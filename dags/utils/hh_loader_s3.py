import os
import requests
import pandas as pd
from datetime import datetime, timezone, time
import logging


# данный класс получает вакансии по API HH за день
class HHVacanciesLoader:
    AREAS_URL = "https://api.hh.ru/areas"
    VACANCIES_URL = "https://api.hh.ru/vacancies"
    HEADERS = {"User-Agent": "Mozilla/5.0"}

    CSV_PATH = "/opt/airflow/dags/data/hh_vacancies.csv"
    S3_BUCKET = "hh_raw"
    S3_PREFIX = "Nataliia_Tarasova/hh_vacancies"

    KEYWORDS = [
        "data engineer", "инженер данных", "sql разработчик", "dba",
        "администратор баз данных", "etl", "bi analyst",
    ]

    def __init__(self, target: str = "s3", aws_conn_id: str = "minios3_conn"):
        self.target = target
        self.aws_conn_id = aws_conn_id if target == "s3" else None

    def get_regions(self):
        logging.info("Запрос списка регионов...")
        resp = requests.get(self.AREAS_URL, headers=self.HEADERS)
        resp.raise_for_status()
        russia = next(a for a in resp.json() if a["name"] == "Россия")
        regions = [region for region in russia["areas"] if not region["areas"]]
        logging.info(f"Найдено {len(regions)} регионов")
        return regions

    def fetch_vacancies(self, region_id, keyword, date):
        date_from = datetime.combine(date, time(0, 0, 0))
        date_to = datetime.combine(date, time(23, 59, 59))

        vacancies, page = [], 0
        while True:
            params = {
                "text": keyword,
                "area": region_id,
                "per_page": 100,
                "page": page,
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
            }
            resp = requests.get(self.VACANCIES_URL, headers=self.HEADERS, params=params, timeout=60)
            if resp.status_code == 403:
                logging.warning(f"Доступ запрещён для region_id={region_id}, keyword={keyword}")
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

            if page >= data.get("pages", 1) - 1:
                break
            page += 1

        df = pd.DataFrame(vacancies)
        if df.empty:
            logging.warning(f"Нет данных для region_id={region_id}, keyword={keyword}")
            return None
        df["update_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        df["id"] = df["id"].astype(str)
        logging.info(f"Найдено {len(df)} вакансий для region_id={region_id}, keyword={keyword}")
        return df
    
    def save_to_csv(self, business_date, mode="w"):
        """Сохраняем CSV, при append добавляем только новые записи к уже существующим.""" 
        regions = self.get_regions()
        all_data = []
        for region in regions:
            for kw in self.KEYWORDS:
                df = self.fetch_vacancies(region["id"], kw, business_date)
                if df is not None:
                    all_data.append(df)

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            logging.info(f"Итого вакансий для загрузки: {len(final_df)}")
        else:
            final_df = pd.DataFrame()
        if self.target == 'csv': 
            if mode == "a" and os.path.exists(self.CSV_PATH): 
                old_df = pd.read_csv(self.CSV_PATH, usecols=["id", "name"]) 
                new_df = pd.concat([old_df, final_df]).drop_duplicates(subset=["id"])
                new_df.to_csv(self.CSV_PATH, header=False, index=False) 
                logging.info(f"➕ Добавлено {len(final_df)} - {len(old_df)} новых строк") 
            else: 
                final_df.to_csv(self.CSV_PATH, header=True, index=False) 
                logging.info(f"💾 Сохранено {len(final_df)} строк (новый файл)") 
                
        else: logging.info(f"Указано неверное значение target: {self.target}")
