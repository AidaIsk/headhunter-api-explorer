{{ config(
    materialized='table',
    schema='gold'
) }}

select
    input_name,
    normalized_input_name,
    entity_id,
    matched_name as best_match,
    matched_normalized_name,
    name_type,
    source_list,
    round(similarity_score::numeric, 4) as similarity_score,
    screening_result
from dbt_staging_gold.sanctions_fuzzy_matches_best
where rn = 1
order by input_name