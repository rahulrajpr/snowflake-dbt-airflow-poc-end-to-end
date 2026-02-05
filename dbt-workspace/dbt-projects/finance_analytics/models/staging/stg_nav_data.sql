{{
    config(
        materialized='table',
        tags=['staging', 'gcs', 'daily_feed']
    )
}}

/*
=============================================================================

PURPOSE:
    - Clean daily Net Asset Value (NAV) pricing data from GCS
    - Validate NAV prices and changes
    - Foundation for fund performance calculations
    - Time series data for NAV trend analysis

BUSINESS CONTEXT:
    - NAV = price per unit of mutual fund
    - Published daily after market close
    - Critical for calculating investment returns
    - Used to value investor holdings

TRANSFORMATION LOGIC:
    1. Validate NAV values (must be positive)
    2. Check for unusual price movements (>10% daily change)
    3. Ensure one record per fund per day
    4. Calculate absolute NAV change (not just percentage)

DEPENDENCIES:
    - Source: landing.vw_nav_data (from GCS CSV files)
    
=============================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('landing', 'vw_nav_data') }}
),

cleaned AS (
    SELECT

        date AS nav_date,
        
        UPPER(TRIM(fund_code)) AS fund_code,
        TRIM(fund_name) AS fund_name,
        
        nav,
        change_percent,
        total_assets,

        nav * (change_percent / 100) AS nav_absolute_change,
        total_assets / 1000000 AS total_assets_millions,
        
        source_file_name,
        source_file_row_number,
        file_content_key,
        file_last_modified_timestamp,
        load_timestamp,
        load_source,
        
        CASE
            WHEN nav <= 0 THEN 'Invalid NAV'
            WHEN ABS(change_percent) > 10 THEN 'Unusual Movement'
            WHEN total_assets <= 0 THEN 'Invalid Assets'
            WHEN nav_date > CURRENT_DATE() THEN 'Future Date'
            ELSE 'Valid'
        END AS data_quality_flag

    FROM source
    WHERE 1=1
        AND date IS NOT NULL
        AND fund_code IS NOT NULL
        AND nav IS NOT NULL
        AND nav > 0
        AND date <= CURRENT_DATE()
)

SELECT * FROM cleaned