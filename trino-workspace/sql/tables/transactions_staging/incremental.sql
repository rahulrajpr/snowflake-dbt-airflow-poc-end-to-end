INSERT INTO hive.default.transactions_staging
SELECT
    transaction_id,
    transaction_number,
    account_id,
    fund_id,
    transaction_type,
    transaction_date,
    settlement_date,
    units,
    price_per_unit,
    amount,
    fees,
    tax_amount,
    net_amount,
    transaction_status,
    channel,
    CAST(notes AS VARCHAR),
    created_at,
    updated_at,
    CURRENT_DATE AS extract_date
FROM postgresql.finance_schema.transactions src
WHERE src.updated_at >
(
    SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01')
    FROM hive.default.transactions_staging
)