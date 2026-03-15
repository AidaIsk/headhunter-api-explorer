{{ config(
    materialized='table',
    schema='silver'
) }}

with primary_names as (

    select
        entity_id,
        primary_name as name_value,
        'primary' as name_type,
        source_list,
        load_timestamp
    from dbt_staging.stg_unsc__entities
    where primary_name is not null

),

alias_names as (

    select
        entity_id,
        alias_name as name_value,
        'alias' as name_type,
        source_list,
        load_timestamp
    from dbt_staging.stg_unsc__aliases
    where alias_name is not null

),

unioned as (

    select * from primary_names
    union all
    select * from alias_names

)

select
    entity_id,
    name_value,
    name_type,
    source_list,
    load_timestamp,

    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    lower(name_value),
                    '[-/]+', ' ', 'g'
                ),
                '[^[:alnum:][:space:]]+', '', 'g'
            ),
            '\s+', ' ', 'g'
        )
    ) as normalized_name

from unioned