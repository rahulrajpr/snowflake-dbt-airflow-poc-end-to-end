CREATE TABLE IF NOT EXISTS hive.default.funds_staging
(
    fund_id BIGINT,
    fund_code VARCHAR,
    fund_name VARCHAR,
    fund_category VARCHAR,
    fund_type VARCHAR,
    manager_id INTEGER,
    inception_date DATE,
    expense_ratio DECIMAL(5,4),
    minimum_investment DECIMAL(15,2),
    aum_millions DECIMAL(15,2),
    benchmark_index VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    extract_date VARCHAR
)
WITH (
    external_location = 'gs://finance-data-landing-bucket-poc/trino-extracts/funds/',
    format = 'PARQUET',
    partitioned_by = ARRAY['extract_date']
)