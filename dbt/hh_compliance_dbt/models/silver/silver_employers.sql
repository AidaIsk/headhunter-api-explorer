{{ config(
    materialized='incremental',
    unique_key='employer_id'
) }}

with source as (

    select
        (employer->>'id')::bigint          as employer_id,
        employer->>'name'                  as name,
        employer->>'url'                   as url,
        (employer->>'trusted')::boolean    as trusted,
        (employer->>'accredited_it_employer')::boolean as accredited_it_employer,
        (employer->>'country_id')::int     as country_id,
        load_dt
    from {{ source('bronze', 'hh_vacancies_bronze') }}
    where employer is not null
      and (employer->>'id') is not null

    {% if is_incremental() %}
        -- Берём только новые батчи.
        -- dbt обновит запись работодателя при конфликте по unique_key='employer_id'.
        -- first_seen_dt / last_seen_dt агрегируются ниже по всем свежим строкам батча.
        and load_dt > (select max(load_dt) from {{ this }})
    {% endif %}

)

select
    employer_id,
    name,
    url,
    trusted,
    accredited_it_employer,
    country_id,
    min(load_dt)::date as first_seen_dt,
    max(load_dt)::date as last_seen_dt

from source
group by 1, 2, 3, 4, 5, 6
