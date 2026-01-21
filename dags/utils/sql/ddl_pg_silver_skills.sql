CREATE TABLE silver.vacancy_skills (
    vacancy_id     BIGINT NOT NULL,
    skill_name     TEXT NOT NULL,
    load_dt        DATE NOT NULL,

    PRIMARY KEY (vacancy_id, skill_name)
);