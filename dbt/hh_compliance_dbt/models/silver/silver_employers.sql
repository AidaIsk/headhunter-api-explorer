{{ config(
    materialized='incremental',
    unique_key='employer_id'
) }}

with new_data as (
    select *
    from {{ source('bronze', 'hh_vacancies_bronze') }}
    where employer is not null and (employer->>'id') is not null
    {% if is_incremental() %}
        and load_dt > (select max(load_dt) from {{ this }})
    {% endif %}
),

combined as (
    select
        (employer->>'id')::bigint as employer_id,
        employer->>'name' as name,
        employer->>'url' as url,
        (employer->>'trusted')::boolean as trusted,
        (employer->>'accredited_it_employer')::boolean as accredited_it_employer,
        (employer->>'country_id')::int as country_id,
        min(load_dt)::date as first_seen_dt,
        max(load_dt)::date as last_seen_dt
    from (
        {% if is_incremental() %}
            select * from {{ this }}   -- уже существующие данные в silver_test
            union all
        {% endif %}
            select * from new_data      -- новые строки из bronze
    ) t
    group by 1,2,3,4,5,6
)

select * from combined