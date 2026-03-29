{{ config(
    materialized='incremental',
    unique_key='vacancy_id',
    alias='vacancy_text',
    schema='silver_test'
) }}

with new_data as (
    select
        id as vacancy_id,
        regexp_replace(coalesce(description, ''), '<[^>]*>', '', 'g') as description_text,
        load_dt
    from {{ source('bronze', 'hh_vacancies_details_bronze') }}
    {% if is_incremental() %}
        where load_dt > (select coalesce(max(load_dt), '1900-01-01') from {{ this }})
    {% endif %}
)

select * from new_data
