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

scored as (

    select
        i.input_name,
        i.normalized_input_name,
        s.entity_id,
        s.name_value as matched_name,
        s.name_type,
        s.source_list,
        dbt_staging_gold.similarity(
            i.normalized_input_name::text,
            s.normalized_name::text
        ) as similarity_score
    from inputs i
    cross join sanctions s

),

matched as (

    select *
    from scored
    where similarity_score > 0.7

),

deduped as (

    select
        *,
        row_number() over (
            partition by input_name, matched_name
            order by similarity_score desc, entity_id
        ) as dedup_rn
    from matched

),

best_per_name as (

    select
        input_name,
        normalized_input_name,
        entity_id,
        matched_name,
        name_type,
        source_list,
        similarity_score
    from deduped
    where dedup_rn = 1

),

ranked as (

    select
        *,
        row_number() over (
            partition by input_name
            order by similarity_score desc, matched_name
        ) as rn
    from best_per_name

)

select
    input_name,
    normalized_input_name,
    entity_id,
    matched_name,
    name_type,
    source_list,
    similarity_score,
    case
        when similarity_score >= 0.95 then 'MATCH'
        when similarity_score >= 0.70 then 'POSSIBLE_MATCH'
        else 'NO_MATCH'
    end as screening_result,
    rn
from ranked
where rn <= 3
order by input_name, rn