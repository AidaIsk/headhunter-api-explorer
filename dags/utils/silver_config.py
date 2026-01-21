from utils.natalia_hh_pg_silver import load_silver_vacancies, load_silver_employers, load_silver_vacancy_skills, load_silver_vacancy_text


SILVER_TABLES = [
    {
        "name": "vacancies",
        "ddl": "vacancies.sql",
        "load_func": load_silver_vacancies,
    },
    {
        "name": "employers",
        "ddl": "employers.sql",
        "load_func": load_silver_employers,
    },
    {
        "name": "vacancy_skills",
        "ddl": "vacancy_skills.sql",
        "load_func": load_silver_vacancy_skills,
    },
    {
        "name": "vacancy_text",
        "ddl": "vacancy_text.sql",
        "load_func": load_silver_vacancy_text,
    },
]
