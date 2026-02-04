CREATE TABLE IF NOT EXISTS hive.default.account_balances_staging
(
    balance_id BIGINT,
    account_id INTEGER,
    balance_date DATE,
    opening_balance DECIMAL(15,2),
    closing_balance DECIMAL(15,2),
    net_deposits DECIMAL(15,2),
    net_withdrawals DECIMAL(15,2),
    net_investment_gain DECIMAL(15,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    extract_date VARCHAR
)
WITH (
    external_location = 'gs://finance-data-landing-bucket-poc/trino-extracts/account_balances/',
    format = 'PARQUET',
    partitioned_by = ARRAY['extract_date']
)