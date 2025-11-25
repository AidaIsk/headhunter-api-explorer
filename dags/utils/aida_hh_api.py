import time
import requests
import pandas as pd
from typing import List, Dict, Any

# Настройки API и фильтров

BASE_URL = "https://api.hh.ru/vacancies"  
PAGE_SIZE = 100                               # сколько записей на странице
RATE_LIMIT_SLEEP = 2                          # пауза между запросами (секунды)

BASE_PARAMS = {
    "area": 40,                                 # 40 = Казахстан
    "text": '"python" AND "sql"'                # вакансии, где встречаются оба слова
}

# Заголовки HTTP-запроса (HH требует указать User-Agent)
HEADERS =  {"User-Agent": "hh-remote-track/0.1 (aida.aitymova@gmail.com)"}

# Запрос одной страницы API

def fetch_page(page: int) -> Dict[str, Any]:
    """
    Отправляет запрос на HH API и возвращает JSON.
    Если запрос упал — возвращает пустой словарь, чтобы не ломать программу.
    """
    params = {
        **BASE_PARAMS,                      # фильтры вакансий
        "page": page,                       # номер страницы
        "per_page": PAGE_SIZE,              # сколько записей на странице
    }
    try:
        response = requests.get(
            BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=30,
        )
        response.raise_for_status()         # проверка успешности ответа
    except requests.exceptions.RequestException as e:
        return {}    
    
    return response.json()

# Извлечение списка вакансий из ответа API

def extract_items_from_response(response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    HH API всегда возвращает вакансии под ключом "items".
    Если его нет — возвращаем пустой список.
    """

    return response_json.get("items", [])

# Запрос нескольких страниц (пагинация)

def fetch_all_items(max_pages: int = 19) -> List[Dict[str, Any]]:
    """
    Обходит страницы API по очереди: page=0, 1, 2, ...
    Останавливается, если:
     - получили пустую страницу
     - достигли max_pages
     - API вернуло меньше PAGE_SIZE записей (признак последней страницы)
    """
    all_items: List[Dict[str, Any]] = []
    page = 0

    # основной цикл пагинации
    while page <= max_pages:

        response_json = fetch_page(page)
        items = extract_items_from_response(response_json)

        # если данных нет — дальше страниц нет
        if not items:
            break

        all_items.extend(items)

        # если меньше, чем 100 — значит последняя страница
        if len(items) < PAGE_SIZE:
            break

        page += 1
        time.sleep(RATE_LIMIT_SLEEP)

    return all_items

# Преобразование списка JSON → DataFrame

def items_to_dataframe(items: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Разворачивает вложенные словари вакансий в плоскую таблицу.
    Фильтрует только важные поля.
    """
    if not items:
        return pd.DataFrame()

    # Разворачиваем JSON в таблицу
    df = pd.json_normalize(items, sep=".")

    # Поля, которые мы хотим оставить для анализа
    COLUMNS_TO_KEEP = [
        "id",
        "name",
        "employer.name",
        "employer.id",
        "employer.trusted",
        "area.name",
        "area.id",
        "address.raw",
        "salary.from",
        "salary.to",
        "salary.currency",
        "experience.name",
        "schedule.name",
        "employment.name",
        "published_at",
        "created_at",
        "archived",
        "description",
        "snippet.requirement",
        "snippet.responsibility",
    ]

    # Оставляем только колонки, которые реально существуют в данных
    df = df[[col for col in COLUMNS_TO_KEEP if col in df.columns]]

    return df

def pipeline_hh_to_csv(output_path: str):

    """
    Главная функция для Airflow: скачивает вакансии,
    преобразует и сохраняет в CSV.
    """

    items = fetch_all_items()

    df = items_to_dataframe(items)
    
    df.to_csv(output_path, index=False)
    
    return output_path

