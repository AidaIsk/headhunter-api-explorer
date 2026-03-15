{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    entity_id,
    nationality,
    source_list,
    load_timestamp
FROM dbt_staging.stg_unsc__nationalities