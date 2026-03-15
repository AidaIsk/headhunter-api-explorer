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
        end as screening_result,

        case
            when s.name_type = 'primary' then 1
            when s.name_type = 'alias' then 2
            else 99
        end as match_priority

    from normalized_inputs i
    left join dbt_staging_silver.sanctions_names s
        on i.normalized_input_name = s.normalized_name

),

ranked as (

    select
        *,
        row_number() over (
            partition by input_id
            order by
                case when screening_result = 'MATCH' then 1 else 2 end,
                match_priority,
                matched_name
        ) as rn
    from matched

)

select
    input_id,
    input_name,
    normalized_input_name,
    entity_id,
    matched_name,
    name_type,
    source_list,
    screening_result
from ranked
where rn = 1
order by input_id