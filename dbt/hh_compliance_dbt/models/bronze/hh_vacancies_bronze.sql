{{ config(
    materialized='incremental',
    unique_key='id'
) }}

-- Источник данных: сюда dbt будет смотреть как source
select *
from {{ source('bronze', 'hh_vacancies_bronze') }}
{% if is_incremental() %}
    -- Берём только новые строки, чтобы не перезаписывать всю таблицу
    where load_dt > (select max(load_dt) from {{ this }})
{% endif %}