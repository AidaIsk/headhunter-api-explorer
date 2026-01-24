CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.vacancies (
    vacancy_id              BIGINT PRIMARY KEY,
    title                   TEXT NOT NULL,
    published_at            TIMESTAMP NOT NULL,

    area_id                 INT NOT NULL,
    area_name               TEXT NOT NULL,

    employer_id             BIGINT NOT NULL,

    salary_from             NUMERIC(18,2),
    salary_to               NUMERIC(18,2),
    salary_currency         TEXT,

    schedule                TEXT,
    employment              TEXT,
    experience              TEXT,

    archived                BOOLEAN NOT NULL,

    search_profile          TEXT,
    expected_risk_category  TEXT,

    load_dt                 DATE NOT NULL
);
