from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup
from datetime import datetime

def load_silver_vacancies(load_dt: str):

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
        WHERE b.load_dt = %s
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

    cur.execute(sql, (load_dt,))
    conn.commit()
    cur.close()
    conn.close()


def load_silver_employers(load_dt: str):

    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    conn = hook.get_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO silver.employers (
            employer_id,
            name,
            url,
            trusted,
            accredited_it_employer,
            country_id,
            first_seen_dt,
            last_seen_dt
        )
        SELECT
            (b.employer->>'id')::bigint                AS employer_id,
            b.employer->>'name'                        AS name,
            b.employer->>'url'                         AS url,
            (b.employer->>'trusted')::boolean          AS trusted,
            (b.employer->>'accredited_it_employer')::boolean
                                                      AS accredited_it_employer,
            (b.employer->>'country_id')::int           AS country_id,
            MIN(b.load_dt)::date                       AS first_seen_dt,
            MAX(b.load_dt)::date                       AS last_seen_dt
        FROM bronze.hh_vacancies_bronze b
        WHERE b.employer IS NOT NULL and b.load_dt = %s
        GROUP BY
            (b.employer->>'id')::bigint,
            b.employer->>'name',
            b.employer->>'url',
            (b.employer->>'trusted')::boolean,
            (b.employer->>'accredited_it_employer')::boolean,
            (b.employer->>'country_id')::int
        ON CONFLICT (employer_id) DO UPDATE SET
            name = EXCLUDED.name,
            url = EXCLUDED.url,
            trusted = EXCLUDED.trusted,
            accredited_it_employer = EXCLUDED.accredited_it_employer,
            country_id = EXCLUDED.country_id,
            first_seen_dt = LEAST(silver.employers.first_seen_dt, EXCLUDED.first_seen_dt),
            last_seen_dt  = GREATEST(silver.employers.last_seen_dt, EXCLUDED.last_seen_dt);
    """

    cur.execute(sql, (load_dt,))
    conn.commit()
    cur.close()
    conn.close()



def load_silver_vacancy_skills(load_dt: str):

    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    conn = hook.get_conn()
    cur = conn.cursor()

    
    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    conn = hook.get_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO silver.vacancy_skills (
            vacancy_id,
            skill_name,
            load_dt
        )
        SELECT
            b.vacancy_id,
            skill ->> 'name' AS skill_name,
            b.load_dt
        FROM bronze.hh_vacancy_details_bronze b,
             jsonb_array_elements(b.key_skills) AS skill
        WHERE b.load_dt = %s
        ON CONFLICT (vacancy_id, skill_name) DO NOTHING;
    """

    cur.execute(sql, (load_dt,))
    conn.commit()
    cur.close()
    conn.close()




def load_silver_vacancy_text(load_dt: str):
    
    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            b.id AS vacancy_id,
            b.description AS description_html,
            b.load_dt AS load_dt
        FROM bronze.hh_vacancies_details_bronze b
        WHERE b.load_dt = %s
    """, (load_dt,))

    rows = cur.fetchall()
    data_to_insert = []

    for vacancy_id, description_html, load_dt in rows:
        if description_html:
            description_text = BeautifulSoup(description_html, "html.parser").get_text(separator="\n").strip()
        else:
            description_text = ""

        data_to_insert.append((vacancy_id, description_text, load_dt))

    # Вставляем в silver.vacancy_text с UPSERT по vacancy_id
    insert_sql = """
        INSERT INTO silver.vacancy_text (
            vacancy_id,
            description_text,
            load_dt
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (vacancy_id) DO UPDATE
        SET description_text = EXCLUDED.description_text,
            load_dt = EXCLUDED.load_dt;
    """

    cur.executemany(insert_sql, data_to_insert)
    conn.commit()
    cur.close()
    conn.close()
