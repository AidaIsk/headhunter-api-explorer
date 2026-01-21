CREATE TABLE silver.vacancy_text (
    vacancy_id         BIGINT PRIMARY KEY,
    description_text   TEXT NOT NULL,
    load_dt            DATE NOT NULL
);