{{ config(materialized='view') }}

with src as (

    select
        -- business key
        id as vacancy_id,

        -- core vacancy attributes used later in Silver
        name as vacancy_name,
        archived as is_archived,
        published_at,
        created_at,
        initial_created_at,

        -- employer (jsonb) kept for employer extraction later
        employer as employer_json,

        -- text fields for vacancy_text model later
        description as description_raw,
        branded_description as branded_description_raw,

        -- skills (jsonb array) for vacancy_skills later
        key_skills as key_skills_json,

        -- compliance / traceability metadata injected during ingestion
        search_profile,
        expected_risk_category,

        -- ingestion technical metadata (lineage / replay)
        load_dt,
        load_type,
        batch_idx,
        inserted_at

    from {{ source('hh_bronze', 'hh_vacancies_details_bronze') }}

)

select * from src
