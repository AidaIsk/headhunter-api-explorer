CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.hh_vacancies_details_coverage_report (
    load_dt DATE PRIMARY KEY,
    load_type TEXT,
    expected_count INT,
    loaded_count INT,
    missing_count INT,
    coverage_pct NUMERIC,
    severity TEXT,
    found_files INT,
    failures_by_reason JSONB,
    inserted_at TIMESTAMPTZ DEFAULT now()
);

