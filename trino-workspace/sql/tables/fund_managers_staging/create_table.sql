
CREATE TABLE IF NOT EXISTS hive.default.fund_managers_staging
(
    manager_id BIGINT,
    manager_code VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    years_experience INTEGER,
    education VARCHAR,
    specialization VARCHAR,
    is_active BOOLEAN,
    hire_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    extract_date VARCHAR
)
WITH (
    external_location = 'gs://finance-data-landing-bucket-poc/trino-extracts/fund_managers/',
    format = 'PARQUET',
    partitioned_by = ARRAY['extract_date']
)