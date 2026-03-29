{{ config(
    materialized='incremental',
    unique_key='employer_id',
    incremental_strategy='merge',
    alias='employers',
    schema='silver_test'
) }}

select
    (employer->>'id')::bigint as employer_id,
    employer->>'name' as name,
    employer->>'url' as url,
    (employer->>'trusted')::boolean as trusted,
    (employer->>'accredited_it_employer')::boolean as accredited_it_employer,
    (employer->>'country_id')::int as country_id,

    min(load_dt)::date as first_seen_dt,
    max(load_dt)::date as last_seen_dt

from {{ source('bronze', 'hh_vacancies_bronze') }}

where employer is not null
  and (employer->>'id') is not null

{% if is_incremental() %}
  and load_dt >= (
        select coalesce(max(last_seen_dt),'1900-01-01')
        from {{ this }}
  )
{% endif %}

group by
    (employer->>'id')::bigint,
    employer->>'name',
    employer->>'url',
    (employer->>'trusted')::boolean,
    (employer->>'accredited_it_employer')::boolean,
    (employer->>'country_id')::int