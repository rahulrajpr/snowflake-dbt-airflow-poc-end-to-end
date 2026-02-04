INSERT INTO hive.default.accounts_staging
SELECT
    account_id,
    account_number,
    investor_id,
    account_type,
    account_status,
    opening_date,
    closing_date,
    total_balance,
    currency,
    created_at,
    updated_at,
    DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d') AS extract_date
FROM postgresql.finance_schema.accounts src
WHERE src.updated_at >
(
    SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01')
    FROM hive.default.accounts_staging
    WHERE updated_at IS NOT NULL
)