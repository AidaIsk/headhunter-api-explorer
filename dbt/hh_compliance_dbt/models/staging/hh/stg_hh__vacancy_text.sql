{{ config(materialized='view') }}

with src as (

    select
        vacancy_id,
        description_raw,
        branded_description_raw,
        load_dt,
        inserted_at
    from {{ ref('stg_hh__vacancy_details') }}

),

normalized as (

    select
        vacancy_id,

        -- keep raw as-is for auditability
        description_raw,
        branded_description_raw,

        -- choose best available text for downstream search/rules
        nullif(trim(coalesce(branded_description_raw, description_raw, '')), '') as text_raw,

        -- lineage
        load_dt as source_load_dt,
        inserted_at as ingested_at
    from src

),

cleaned as (

    select
        vacancy_id,
        description_raw,
        branded_description_raw,
        text_raw,

        -- simple HTML strip (good enough for v0):
        -- 1) remove tags like <p>, <br>, <ul>...
        -- 2) replace &nbsp; with space
        -- 3) collapse repeated whitespace
        case
            when text_raw is null then null
            else
                regexp_replace(
                    regexp_replace(
                        regexp_replace(text_raw, '<[^>]+>', ' ', 'g'),
                        '&nbsp;', ' ', 'g'
                    ),
                    '\s+', ' ', 'g'
                )
        end as text_clean,

        source_load_dt,
        ingested_at
    from normalized

)

select
    vacancy_id,
    description_raw,
    branded_description_raw,
    text_raw,
    nullif(trim(text_clean), '') as text_clean,
    source_load_dt,
    ingested_at
from cleaned
