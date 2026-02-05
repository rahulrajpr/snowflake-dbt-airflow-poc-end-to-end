{{
    config(
        materialized='table',
        tags=['staging', 'postgres', 'dimension']
    )
}}

/*
=============================================================================

PURPOSE:
    - Clean and standardize mutual fund master data
    - Calculate fund age and performance tenure
    - Validate expense ratios and minimum investments
    - Foundation for fund dimension table

BUSINESS CONTEXT:
    - Master list of all investable mutual funds
    - Core dimension for fund performance and comparison analytics
    - Expense ratio critical for cost analysis
    - AUM (Assets Under Management) tracked at fund level

TRANSFORMATION LOGIC:
    1. Standardize fund categories and types
    2. Calculate fund age (years since inception)
    3. Validate expense ratios (typically 0.01% to 3%)
    4. Format AUM to millions for readability

DEPENDENCIES:
    - Source: landing.vw_funds

=============================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('landing', 'vw_funds') }}
),

cleaned AS (
    SELECT
        fund_id,
        
        UPPER(TRIM(fund_code)) AS fund_code,
        TRIM(fund_name) AS fund_name,
        
        INITCAP(TRIM(fund_category)) AS fund_category,
        UPPER(TRIM(fund_type)) AS fund_type,
        
        manager_id,
        
        inception_date,
        DATEDIFF(YEAR, inception_date, CURRENT_DATE()) AS fund_age_years,
        expense_ratio,
        expense_ratio * 100 AS expense_ratio_bps,
        minimum_investment,
        aum_millions,
        TRIM(benchmark_index) AS benchmark_index,

        is_active,
        
        created_at,
        updated_at,
        load_timestamp,
        
        CASE
            WHEN expense_ratio < 0 OR expense_ratio > 0.05 THEN 'Invalid Expense Ratio'
            WHEN minimum_investment < 0 THEN 'Invalid Min Investment'
            WHEN aum_millions < 0 THEN 'Invalid AUM'
            WHEN inception_date > CURRENT_DATE() THEN 'Future Inception Date'
            ELSE 'Valid'
        END AS data_quality_flag

    FROM source
    WHERE 1=1
        AND fund_id IS NOT NULL
        AND fund_code IS NOT NULL
        AND fund_name IS NOT NULL
        AND is_active = TRUE
)

SELECT * FROM cleaned