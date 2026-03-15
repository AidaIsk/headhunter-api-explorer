{{ config(
    materialized='table',
    schema='gold'
) }}

with input_names as (

    select
        input_id,
        input_name
    from demo.sanctions_screening_inputs

),

normalized_inputs as (

    select
        input_id,
        input_name,

        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        lower(input_name),
                        '[-/]+', ' ', 'g'
                    ),
                    '[^[:alnum:][:space:]]+', '', 'g'
                ),
                '\s+', ' ', 'g'
            )
        ) as normalized_input_name

    from input_names

),

matched as (

    select
        i.input_id,
        i.input_name,
        i.normalized_input_name,
        s.entity_id,
        s.name_value as matched_name,
        s.name_type,
        s.source_list,
        case
            when s.entity_id is not null then 'MATCH'
            else 'NO_MATCH'
        end as screening_result
    from normalized_inputs i
    left join dbt_staging_silver.sanctions_names s
        on i.normalized_input_name = s.normalized_name

)

select *
from matched
order by input_id, name_type nulls last