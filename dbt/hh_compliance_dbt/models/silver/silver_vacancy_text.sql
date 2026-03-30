{{ config(
    materialized='incremental',
    unique_key='vacancy_id'
) }}

with source as (

    select
        id            as vacancy_id,
        description,
        load_dt
    from {{ source('bronze', 'hh_vacancies_details_bronze') }}

    {% if is_incremental() %}
        -- Берём только батчи новее последней загрузки.
        -- dbt сам смержит новые строки с существующими через unique_key='vacancy_id':
        -- если вакансия уже есть — обновит, нет — вставит. Ручной UNION ALL не нужен.
        where load_dt > (select max(load_dt) from {{ this }})
    {% endif %}

)

select
    vacancy_id,
    -- Срезаем HTML-теги: в Bronze хранится сырой HTML, для риск-сигналов нужен чистый текст
    regexp_replace(coalesce(description, ''), '<[^>]*>', '', 'g') as description_text,
    load_dt

from source
