from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_silver_vacancies():
    
    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    conn = hook.get_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO silver.vacancies (
            vacancy_id,
            title,
            published_at,
            area_id,
            area_name,
            employer_id,
            salary_from,
            salary_to,
            salary_currency,
            schedule,
            employment,
            experience,
            archived,
            active_from,
            active_to,
            load_dt,
            search_profile,
            expected_risk_category
        )
        SELECT
            b.id                                   AS vacancy_id,
            b.name                                 AS title,
            b.published_at                         AS published_at,
            (b.area->>'id')::int                   AS area_id,
            b.area->>'name'                        AS area_name,
            (b.employer->>'id')::bigint            AS employer_id,
            (b.salary->>'from')::numeric           AS salary_from,
            (b.salary->>'to')::numeric             AS salary_to,
            b.salary->>'currency'                  AS salary_currency,
            b.schedule->>'name'                      AS schedule,
            b.employment->>'name'                    AS employment,
            b.experience->>'name'                    AS experience,
            b.archived                             AS archived,
            b.active_from                          AS active_from,
            b.active_to                            AS active_to,
            b.load_dt                              AS load_dt,
            b.search_profile                       AS search_profile,
            b.expected_risk_category               AS expected_risk_category
        FROM bronze.hh_vacancies_bronze b
        ON CONFLICT (vacancy_id) DO UPDATE SET
            title = EXCLUDED.title,
            published_at = EXCLUDED.published_at,
            area_id = EXCLUDED.area_id,
            area_name = EXCLUDED.area_name,
            employer_id = EXCLUDED.employer_id,
            salary_from = EXCLUDED.salary_from,
            salary_to = EXCLUDED.salary_to,
            salary_currency = EXCLUDED.salary_currency,
            schedule = EXCLUDED.schedule,
            employment = EXCLUDED.employment,
            experience = EXCLUDED.experience,
            archived = EXCLUDED.archived,
            active_from = EXCLUDED.active_from,
            active_to = EXCLUDED.active_to,
            load_dt = EXCLUDED.load_dt,
            search_profile = EXCLUDED.search_profile,
            expected_risk_category = EXCLUDED.expected_risk_category;
    """

    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()



def load_silver_employers():


def load_silver_vacancy_skills():


def load_silver_vacancy_text():
