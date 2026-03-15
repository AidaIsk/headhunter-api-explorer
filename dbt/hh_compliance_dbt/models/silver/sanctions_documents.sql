{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    entity_id,
    doc_type,
    doc_number,
    source_list,
    load_timestamp
FROM dbt_staging.stg_unsc__documents