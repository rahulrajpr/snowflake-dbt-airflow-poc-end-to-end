
INSERT INTO hive.default.fund_managers_staging
SELECT
    manager_id,
    manager_code,
    first_name,
    last_name,
    email,
    years_experience,
    education,
    specialization,
    is_active,
    hire_date,
    created_at,
    updated_at,
    DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d') AS extract_date
FROM postgresql.finance_schema.fund_managers src
WHERE src.updated_at >
(
    SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01')
    FROM hive.default.fund_managers_staging
    WHERE updated_at IS NOT NULL
)