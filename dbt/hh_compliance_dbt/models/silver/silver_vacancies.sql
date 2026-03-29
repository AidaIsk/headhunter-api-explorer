{{ config(
    materialized='incremental',
    unique_key='vacancy_id',
    alias='vacancies',
    schema='silver_test'
) }}

with bronze as (
    select * from {{ source('bronze', 'hh_vacancies_bronze') }}
)

select
    id as vacancy_id,
    name as title,
    published_at,
    (area->>'id')::int as area_id,
    area->>'name' as area_name,
    (employer->>'id')::bigint as employer_id,
    (salary->>'from')::numeric as salary_from,
    (salary->>'to')::numeric as salary_to,
    salary->>'currency' as salary_currency,
    schedule->>'name' as schedule,
    employment->>'name' as employment,
    experience->>'name' as experience,
    archived,
    active_from,
    active_to,
    load_dt,
    search_profile,
    expected_risk_category
from bronze
where employer is not null
  and (employer->>'id') is not null
  {% if is_incremental() %}
    and load_dt > (select coalesce(max(load_dt), '1900-01-01') from {{ this }})
  {% endif %}