{{ config(
    materialized='table',  
    alias='vacancy_risk_signals',
    schema='gold_test',
) }}

with base as (

    select
        v.vacancy_id,
        v.load_dt,
        v.employer_id,
        e.name as employer_name,
        v.area_name,
        v.title,
        v.published_at,
        not v.archived as is_active,
        v.salary_from,
        v.salary_to,
        v.expected_risk_category,
        t.description_text,

        /* ----- signals ----- */
        case
            when v.employer_id is null or e.name is null or e.name = ''
            then 1 else 0
        end as risk_signal_missing_employer_info,

        case
            when t.description_text is null or length(t.description_text) < 300
            then 1 else 0
        end as risk_signal_vague_job_description,

        case
            when v.expected_risk_category in ('crypto','gambling','payments')
            then 1 else 0
        end as risk_signal_risk_domain_profile,

        case
            when v.salary_from >= 500000 or v.salary_to >= 700000
            then 1 else 0
        end as risk_signal_high_salary_outlier,

        0 as risk_signal_sanctions_match_employer

    from {{ ref('silver_vacancies') }} v
    left join {{ ref('silver_vacancy_text') }} t
        on v.vacancy_id = t.vacancy_id
       and v.load_dt = t.load_dt
    left join {{ ref('silver_employers') }} e
        on v.employer_id = e.employer_id
)

select
    *,
    
    /* ---------- агрегаты сигналов ---------- */
    (risk_signal_high_salary_outlier
     + risk_signal_vague_job_description
     + risk_signal_risk_domain_profile) as risk_warning_count,

    (risk_signal_missing_employer_info
     + risk_signal_sanctions_match_employer) as risk_critical_count,

    /* ---------- risk level ---------- */
    case
        when risk_signal_sanctions_match_employer = 1
          or risk_signal_missing_employer_info = 1
        then 'CRITICAL'
        when risk_signal_high_salary_outlier = 1
          or risk_signal_vague_job_description = 1
          or risk_signal_risk_domain_profile = 1
        then 'WARNING'
        else 'OK'
    end as risk_level,

    /* ---------- downstream ---------- */
    case
        when risk_signal_sanctions_match_employer = 1 then 'alert'
        when risk_signal_missing_employer_info = 1 then 'manual_review'
        when risk_signal_high_salary_outlier = 1
          or risk_signal_vague_job_description = 1
          or risk_signal_risk_domain_profile = 1
        then 'analytics'
        else 'monitor'
    end as downstream_action

from base