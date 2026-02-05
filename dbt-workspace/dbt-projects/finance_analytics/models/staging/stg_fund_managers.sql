{{
    config(
        materialized='table',
        tags=['staging', 'postgres', 'dimension', 'pii']
    )
}}

/*
=============================================================================

PURPOSE:
    - Clean fund manager master data
    - Calculate manager tenure and experience
    - Foundation for manager dimension and performance attribution
    - Contains PII (email, name)

BUSINESS CONTEXT:
    - Fund managers responsible for investment decisions
    - Manager experience correlated with fund performance
    - One manager can oversee multiple funds
    - Manager changes tracked for performance impact analysis

TRANSFORMATION LOGIC:
    1. Standardize names (proper casing)
    2. Calculate total tenure at firm
    3. Validate experience levels (typically 5-40 years)
    4. Clean specialization categories

DEPENDENCIES:
    - Source: landing.vw_fund_managers
    
=============================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('landing', 'vw_fund_managers') }}
),

cleaned AS (
    SELECT
        manager_id,
        UPPER(TRIM(manager_code)) AS manager_code,
        
        TRIM(INITCAP(first_name)) AS first_name,
        TRIM(INITCAP(last_name)) AS last_name,
        TRIM(first_name) || ' ' || TRIM(last_name) AS full_name,
        LOWER(TRIM(email)) AS email,
        
        years_experience,
        TRIM(education) AS education,
        TRIM(INITCAP(specialization)) AS specialization,
        
        hire_date,
        DATEDIFF(YEAR, hire_date, CURRENT_DATE()) AS tenure_years,
        is_active,
        
        created_at,
        updated_at,
        load_timestamp,
        
        CASE
            WHEN years_experience < 5 THEN 'Junior'
            WHEN years_experience BETWEEN 5 AND 15 THEN 'Mid-Level'
            WHEN years_experience BETWEEN 15 AND 25 THEN 'Senior'
            ELSE 'Expert'
        END AS experience_level,
        
        CASE
            WHEN years_experience < 0 OR years_experience > 50 THEN 'Invalid Experience'
            WHEN hire_date > CURRENT_DATE() THEN 'Future Hire Date'
            WHEN email IS NULL THEN 'Missing Email'
            ELSE 'Valid'
        END AS data_quality_flag

    FROM source
    WHERE 1=1
        AND manager_id IS NOT NULL
        AND manager_code IS NOT NULL
        AND first_name IS NOT NULL
        AND last_name IS NOT NULL
        AND is_active = TRUE
)

SELECT * FROM cleaned