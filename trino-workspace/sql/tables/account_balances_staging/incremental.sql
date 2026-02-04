INSERT INTO hive.default.account_balances_staging
SELECT
    balance_id,
    account_id,
    balance_date,
    opening_balance,
    closing_balance,
    net_deposits,
    net_withdrawals,
    net_investment_gain,
    created_at,
    updated_at,
    DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d') AS extract_date
FROM postgresql.finance_schema.account_balances src
WHERE src.updated_at >
(
    SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01')
    FROM hive.default.account_balances_staging
    WHERE updated_at IS NOT NULL
)