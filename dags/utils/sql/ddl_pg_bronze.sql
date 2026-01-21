CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.hh_vacancies_bronze (
    id BIGINT PRIMARY KEY,

    premium BOOLEAN,
    name TEXT,
    department JSONB,
    has_test BOOLEAN,
    response_letter_required BOOLEAN,

    area JSONB,
    salary JSONB,
    salary_range JSONB,
    type JSONB,
    address JSONB,

    response_url TEXT,
    sort_point_distance NUMERIC,

    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    archived BOOLEAN,
    active_from DATE,
    active_to DATE DEFAULT '9999-01-01',

    apply_alternate_url TEXT,
    show_logo_in_search BOOLEAN,
    show_contacts BOOLEAN,
    insider_interview JSONB,

    url TEXT,
    alternate_url TEXT,
    relations JSONB,

    employer JSONB,
    snippet JSONB,
    contacts JSONB,

    schedule JSONB,
    working_days JSONB,
    working_time_intervals JSONB,
    working_time_modes JSONB,
    accept_temporary BOOLEAN,
    fly_in_fly_out_duration JSONB,

    work_format JSONB,
    working_hours JSONB,
    work_schedule_by_days JSONB,
    night_shifts BOOLEAN,

    professional_roles JSONB,
    accept_incomplete_resumes BOOLEAN,

    experience JSONB,
    employment JSONB,
    employment_form JSONB,

    internship BOOLEAN,
    adv_response_url TEXT,
    is_adv_vacancy BOOLEAN,
    adv_context JSONB,

    branding JSONB,
    brand_snippet JSONB,

    -- enrichment
    search_profile TEXT,
    expected_risk_category TEXT,

    load_dt DATE, --вместо load_dt
    load_type TEXT,

    inserted_at TIMESTAMPTZ DEFAULT now() --дата загрузки
);
