{{ config(
    materialized='incremental',
    unique_key='vacancy_id'
) }}

with new_data as (
    select *
    from {{ source('bronze', 'hh_vacancies_details_bronze') }}
    {% if is_incremental() %}
        where load_dt > (select max(load_dt) from {{ this }})
    {% endif %}
),

combined as (
    select
        id as vacancy_id,
        regexp_replace(coalesce(description, ''), '<[^>]*>', '', 'g') as description_text,
        load_dt
    from (
        {% if is_incremental() %}
            select * from {{ this }}  -- уже существующие данные в silver_test
            union all
        {% endif %}
            select * from new_data     -- новые строки из bronze
    ) t
)

select * from combined