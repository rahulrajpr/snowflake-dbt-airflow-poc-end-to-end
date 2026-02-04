INSERT INTO hive.default.investors_staging
SELECT
    investor_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    registration_date,
    kyc_status,
    risk_profile,
    customer_segment,
    address_line1,
    address_line2,
    city,
    state,
    postal_code,
    country,
    is_active,
    created_at,
    updated_at,
    DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d') AS extract_date
FROM postgresql.finance_schema.investors src
WHERE src.updated_at >
(
    SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01')
    FROM hive.default.investors_staging
)