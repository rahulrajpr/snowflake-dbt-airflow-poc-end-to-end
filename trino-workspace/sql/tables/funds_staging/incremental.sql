INSERT INTO hive.default.funds_staging
SELECT
    fund_id,
    fund_code,
    fund_name,
    fund_category,
    fund_type,
    manager_id,
    inception_date,
    expense_ratio,
    minimum_investment,
    aum_millions,
    benchmark_index,
    is_active,
    created_at,
    updated_at,
    DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d') AS extract_date
FROM postgresql.finance_schema.funds src
WHERE src.updated_at >
(
    SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01')
    FROM hive.default.funds_staging
    WHERE updated_at IS NOT NULL
)