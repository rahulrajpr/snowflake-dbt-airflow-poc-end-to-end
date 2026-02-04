INSERT INTO hive.default.fund_holdings_staging
SELECT
    holding_id,
    account_id,
    fund_id,
    units_held,
    average_cost_per_unit,
    current_value,
    last_transaction_date,
    created_at,
    updated_at,
    DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d') AS extract_date
FROM postgresql.finance_schema.fund_holdings src
WHERE src.updated_at >
(
    SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01')
    FROM hive.default.fund_holdings_staging
    WHERE updated_at IS NOT NULL
)