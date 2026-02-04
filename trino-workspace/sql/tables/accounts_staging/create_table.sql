CREATE TABLE IF NOT EXISTS hive.default.accounts_staging
(
    account_id BIGINT,
    account_number VARCHAR,
    investor_id INTEGER,
    account_type VARCHAR,
    account_status VARCHAR,
    opening_date DATE,
    closing_date DATE,
    total_balance DECIMAL(15,2),
    currency VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    extract_date VARCHAR
)
WITH (
    external_location = 'gs://finance-data-landing-bucket-poc/trino-extracts/accounts/',
    format = 'PARQUET',
    partitioned_by = ARRAY['extract_date']
)