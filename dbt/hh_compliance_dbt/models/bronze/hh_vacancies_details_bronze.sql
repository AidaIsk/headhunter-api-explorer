{{ config(
    materialized='incremental',
    unique_key='id'
) }}

select *
from {{ source('bronze', 'hh_vacancies_details_bronze') }}
{% if is_incremental() %}
    -- Берём только новые строки по load_dt
    where load_dt > (select max(load_dt) from {{ this }})
{% endif %}