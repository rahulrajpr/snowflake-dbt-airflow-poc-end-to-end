{{
    config(
        materialized='table',
        data_retention_time_in_days=3,
        tags=['staging', 'postgres', 'pii']
    )
}}

/*
=============================================================================

PURPOSE:
    - Clean and standardize investor master data from PostgreSQL source
    - Apply data quality filters (remove inactive/invalid records)
    - Standardize text fields (proper casing, trimming)
    - Type casting and null handling
    - Foundation for investor dimension table

BUSINESS CONTEXT:
    - Investors are individuals/entities who hold investment accounts
    - Core dimension for all investor analytics and reporting
    - Contains PII (email, phone, address) - handle with care

TRANSFORMATION LOGIC:
    1. Remove records with missing critical fields
    2. Standardize text fields to consistent format
    3. Calculate derived fields (full_name, age)
    4. Filter out test/invalid data

=============================================================================
*/

WITH source AS 
    (
    SELECT * FROM {{ source('landing', 'vw_investors') }}
    ),
cleaned AS 
    (
    SELECT
        investor_id,
        TRIM(INITCAP(first_name)) AS first_name,
        TRIM(INITCAP(last_name)) AS last_name,
        TRIM(first_name) || ' ' || TRIM(last_name) AS full_name,
        LOWER(TRIM(email)) AS email,
        phone,
        date_of_birth,
        DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) AS age,

        registration_date,
        UPPER(TRIM(kyc_status)) AS kyc_status,
        INITCAP(TRIM(risk_profile)) AS risk_profile,
        INITCAP(TRIM(customer_segment)) AS customer_segment,
        
        TRIM(address_line1) AS address_line1,
        TRIM(address_line2) AS address_line2,
        TRIM(INITCAP(city)) AS city,
        TRIM(UPPER(state)) AS state,
        TRIM(postal_code) AS postal_code,
        TRIM(UPPER(country)) AS country,
        
        is_active,
        
        created_at,
        updated_at,
        load_timestamp,
        
        CASE
            WHEN email IS NULL THEN 'Missing Email'
            WHEN date_of_birth IS NULL THEN 'Missing DOB'
            WHEN kyc_status = 'REJECTED' THEN 'KYC Rejected'
            ELSE 'Valid'
        END AS data_quality_flag

    FROM source
    WHERE 1=1
        AND investor_id IS NOT NULL
        AND first_name IS NOT NULL
        AND last_name IS NOT NULL
        AND is_active = TRUE  -- Only active investors
        AND NOT (email LIKE '%test%')  -- Exclude test data
        AND NOT (email LIKE '%@example.com')  -- Exclude dummy emails
)

SELECT * FROM cleaned