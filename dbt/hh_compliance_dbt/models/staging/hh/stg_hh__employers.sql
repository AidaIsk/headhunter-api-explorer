{{ config(materialized='view') }}

with src as (

    select
        vacancy_id,
        employer_json,
        load_dt,
        inserted_at
    from {{ ref('stg_hh__vacancy_details') }}
    where employer_json is not null

),

parsed as (

    select
        (employer_json ->> 'id')::bigint                      as employer_id,
        employer_json ->> 'name'                              as employer_name,
        employer_json ->> 'alternate_url'                     as employer_alternate_url,
        employer_json ->> 'url'                               as employer_api_url,
        employer_json ->> 'vacancies_url'                     as employer_vacancies_url,
        (employer_json ->> 'trusted')::boolean                as is_trusted,
        (employer_json ->> 'accredited_it_employer')::boolean as is_accredited_it_employer,
        (employer_json ->> 'is_identified_by_esia')::boolean  as is_identified_by_esia,
        (employer_json ->> 'country_id')::int                 as country_id,

        -- keep logo urls as jsonb for now (can be unpacked later if needed)
        employer_json -> 'logo_urls'                          as logo_urls_json,

        load_dt,
        inserted_at
    from src

),

dedup as (

    select distinct on (employer_id)
        employer_id,
        employer_name,
        employer_alternate_url,
        employer_api_url,
        employer_vacancies_url,
        is_trusted,
        is_accredited_it_employer,
        is_identified_by_esia,
        country_id,
        logo_urls_json,

        -- lineage
        load_dt as source_load_dt,
        inserted_at as ingested_at
    from parsed
    where employer_id is not null
    order by employer_id, load_dt desc, inserted_at desc

)

select * from dedup
