CREATE TABLE IF NOT EXISTS hive.default.fund_holdings_staging
(
    holding_id BIGINT,
    account_id INTEGER,
    fund_id INTEGER,
    units_held DECIMAL(15,4),
    average_cost_per_unit DECIMAL(15,4),
    current_value DECIMAL(15,2),
    last_transaction_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    extract_date VARCHAR
)
WITH (
    external_location = 'gs://finance-data-landing-bucket-poc/trino-extracts/fund_holdings/',
    format = 'PARQUET',
    partitioned_by = ARRAY['extract_date']
)