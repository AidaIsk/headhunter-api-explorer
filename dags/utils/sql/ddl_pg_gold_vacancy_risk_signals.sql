CREATE SCHEMA IF NOT EXISTS gold;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.vacancy_risk_signals AS

WITH base AS (

SELECT
    v.vacancy_id,
    v.load_dt,

    v.employer_id,
    e.name AS employer_name,

    v.area_name,
    v.title,
    v.published_at,

    NOT v.archived AS is_active,

    v.salary_from,
    v.salary_to,
    v.expected_risk_category,

    t.description_text,

    /* ----- signals ----- */

    CASE
        WHEN v.employer_id IS NULL
          OR e.name IS NULL
          OR e.name = ''
        THEN 1 ELSE 0
    END AS risk_signal_missing_employer_info,

    CASE
        WHEN t.description_text IS NULL
          OR length(t.description_text) < 300
        THEN 1 ELSE 0
    END AS risk_signal_vague_job_description,

    CASE
        WHEN v.expected_risk_category IN ('crypto','gambling','payments')
        THEN 1 ELSE 0
    END AS risk_signal_risk_domain_profile,

    CASE
        WHEN v.salary_from >= 500000
          OR v.salary_to >= 700000
        THEN 1 ELSE 0
    END AS risk_signal_high_salary_outlier,

    0 AS risk_signal_sanctions_match_employer

FROM silver.vacancies v
LEFT JOIN silver.vacancy_text t
    ON v.vacancy_id = t.vacancy_id
   AND v.load_dt = t.load_dt
LEFT JOIN silver.employers e
    ON v.employer_id = e.employer_id

)

SELECT
    *,
    
    /* ---------- агрегаты сигналов ---------- */

    (risk_signal_high_salary_outlier
     + risk_signal_vague_job_description
     + risk_signal_risk_domain_profile) AS risk_warning_count,

    (risk_signal_missing_employer_info
     + risk_signal_sanctions_match_employer) AS risk_critical_count,

    /* ---------- risk level ---------- */

    CASE
        WHEN risk_signal_sanctions_match_employer = 1
          OR risk_signal_missing_employer_info = 1
        THEN 'CRITICAL'

        WHEN risk_signal_high_salary_outlier = 1
          OR risk_signal_vague_job_description = 1
          OR risk_signal_risk_domain_profile = 1
        THEN 'WARNING'

        ELSE 'OK'
    END AS risk_level,

    /* ---------- downstream ---------- */

    CASE
        WHEN risk_signal_sanctions_match_employer = 1
        THEN 'alert'

        WHEN risk_signal_missing_employer_info = 1
        THEN 'manual_review'

        WHEN
            risk_signal_high_salary_outlier = 1
         OR risk_signal_vague_job_description = 1
         OR risk_signal_risk_domain_profile = 1
        THEN 'analytics'

        ELSE 'monitor'
    END AS downstream_action

FROM base;
