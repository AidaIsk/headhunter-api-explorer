{{ config(
    materialized='table',
    schema='gold'
) }}

with inputs as (

    select
        input_name,
        trim(
            regexp_replace(
                lower(
                    regexp_replace(
                        regexp_replace(input_name, '[/\-]', ' ', 'g'),
                        '[^a-zA-Z0-9\s]',
                        '',
                        'g'
                    )
                ),
                '\s+',
                ' ',
                'g'
            )
        ) as normalized_input_name
    from demo.sanctions_screening_inputs

),

sanctions as (

    select
        entity_id,
        name_value,
        name_type,
        source_list,
        normalized_name
    from dbt_staging_silver.sanctions_names

),

matched as (

    select
        i.input_name,
        i.normalized_input_name,
        s.entity_id,
        s.name_value as matched_name,
        s.name_type,
        s.source_list,
        similarity(i.normalized_input_name, s.normalized_name) as similarity_score
    from inputs i
    join sanctions s
      on similarity(i.normalized_input_name, s.normalized_name) > 0.7

),

ranked as (

    select
        *,
        row_number() over (
            partition by input_name
            order by similarity_score desc, matched_name
        ) as rn
    from matched

)

select
    input_name,
    normalized_input_name,
    entity_id,
    matched_name,
    name_type,
    source_list,
    similarity_score,
    rn
from ranked
where rn <= 3
order by input_name, rn