{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    entity_id,
    alias_name,
    source_list,
    load_timestamp
FROM dbt_staging.stg_unsc__aliases