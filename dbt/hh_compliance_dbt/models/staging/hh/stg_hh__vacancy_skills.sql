{{ config(materialized='view') }}

with src as (

    select
        vacancy_id,
        key_skills_json,
        load_dt,
        inserted_at
    from {{ ref('stg_hh__vacancy_details') }}
    where key_skills_json is not null

),

exploded as (

    select
        vacancy_id,
        jsonb_array_elements(key_skills_json) as skill_obj,
        load_dt,
        inserted_at
    from src

),

normalized as (

    select
        vacancy_id,
        nullif(trim(skill_obj ->> 'name'), '') as skill_name,

        -- lineage
        load_dt as source_load_dt,
        inserted_at as ingested_at
    from exploded

)

select distinct
    vacancy_id,
    skill_name,
    source_load_dt,
    ingested_at
from normalized
where skill_name is not null
