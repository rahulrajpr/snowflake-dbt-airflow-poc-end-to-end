CREATE TABLE IF NOT EXISTS hive.default.transactions_staging (
    transaction_id BIGINT,
    transaction_number VARCHAR,
    account_id BIGINT,
    fund_id BIGINT,
    transaction_type VARCHAR,
    transaction_date DATE,
    settlement_date DATE,
    units DOUBLE,
    price_per_unit DOUBLE,
    amount DOUBLE,
    fees DOUBLE,
    tax_amount DOUBLE,
    net_amount DOUBLE,
    transaction_status VARCHAR,
    channel VARCHAR,
    notes VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    extract_date DATE     -- ✅ REQUIRED
)
WITH (
    format = 'PARQUET',
    external_location = 'gs://finance-data-landing-bucket-poc/trino-extracts/transactions/',
    partitioned_by = ARRAY['extract_date']
)