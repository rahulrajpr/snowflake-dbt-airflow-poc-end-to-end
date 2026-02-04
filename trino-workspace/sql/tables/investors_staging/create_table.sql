CREATE TABLE IF NOT EXISTS hive.default.investors_staging
(
    investor_id BIGINT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    date_of_birth DATE,
    registration_date DATE,
    kyc_status VARCHAR,
    risk_profile VARCHAR,
    customer_segment VARCHAR,
    address_line1 VARCHAR,
    address_line2 VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    extract_date VARCHAR
)
WITH (
    external_location = 'gs://finance-data-landing-bucket-poc/trino-extracts/investors/',
    format = 'PARQUET',
    partitioned_by = ARRAY['extract_date']
)
