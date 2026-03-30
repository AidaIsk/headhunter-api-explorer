{{ config(
    materialized='incremental',
    unique_key=['vacancy_id', 'skill_name']
) }}

with source as (

    select
        id      as vacancy_id,
        key_skills,
        load_dt
    from {{ source('bronze', 'hh_vacancies_details_bronze') }}

    {% if is_incremental() %}
        -- Берём только новые батчи.
        -- dbt смержит по составному ключу (vacancy_id, skill_name):
        -- новый скилл вставится, существующий — обновится без дублей.
        where load_dt > (select max(load_dt) from {{ this }})
    {% endif %}

)

select
    vacancy_id,
    -- Разворачиваем массив навыков: одна строка = один скилл одной вакансии
    skill->>'name' as skill_name,
    load_dt

from source,
    jsonb_array_elements(key_skills) as skill
