{{
    config(
        materialized='table',
        tags=['staging', 'postgres']
    )
}}

/*
=============================================================================

PURPOSE:
    - Clean and standardize account data from PostgreSQL
    - Validate account status and types
    - Calculate account age and tenure metrics
    - Foundation for account dimension and portfolio analysis

BUSINESS CONTEXT:
    - Accounts link investors to their investment portfolios
    - One investor can have multiple accounts (retirement, savings, etc.)
    - Account status determines transaction eligibility

TRANSFORMATION LOGIC:
    1. Standardize account types and status values
    2. Calculate account tenure (days since opening)
    3. Validate currency codes
    4. Filter closed accounts (optional business rule)

DEPENDENCIES:
    - Source: landing.vw_accounts
    
=============================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('landing', 'vw_accounts') }}
),

cleaned AS (
    SELECT
        account_id,
        UPPER(TRIM(account_number)) AS account_number,
        investor_id,
        
        UPPER(TRIM(account_type)) AS account_type,
        UPPER(TRIM(account_status)) AS account_status,
        UPPER(TRIM(currency)) AS currency,
        
        opening_date,
        closing_date,
        DATEDIFF(DAY, opening_date, COALESCE(closing_date, CURRENT_DATE())) AS account_age_days,
        
        total_balance,
        
        created_at,
        updated_at,
        load_timestamp,
        
        CASE 
            WHEN account_status = 'ACTIVE' AND closing_date IS NULL THEN TRUE
            ELSE FALSE
        END AS is_currently_active,
        
        CASE
            WHEN total_balance < 0 THEN 'Negative Balance'
            WHEN account_status = 'CLOSED' AND total_balance > 0 THEN 'Closed with Balance'
            ELSE 'Valid'
        END AS data_quality_flag

    FROM source
    
    WHERE 1=1
           AND account_id IS NOT NULL
           AND account_number IS NOT NULL
           AND investor_id IS NOT NULL
           AND account_type IN ('JOINT', 'IRA', 'CORPORATE', 'REGULAR','ROTH_IRA')
           AND currency IN ('USD', 'EUR', 'GBP', 'INR')
)

SELECT * FROM cleaned




