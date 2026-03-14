{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    entity_id,
    data_id,
    entity_type,
    primary_name,
    un_reference_number,
    listed_on,
    source_list,
    load_timestamp
FROM dbt_staging.stg_unsc__entities