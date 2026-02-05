{{
    config(
        materialized='table',
        tags=['staging', 'gcs', 'reference_data']
    )
}}

/*
=============================================================================

PURPOSE:
    - Clean extended fund metadata from external GCS source
    - Enrich fund data with investment objectives and risk ratings
    - Foundation for fund classification and recommendation engine
    - Supplements core fund data with descriptive attributes

BUSINESS CONTEXT:
    - Extended fund attributes not in transactional system
    - Investment objective = fund's stated strategy
    - Risk rating from external rating agency
    - Used for fund comparison and investor suitability matching

TRANSFORMATION LOGIC:
    1. Standardize fund categories to match internal taxonomy
    2. Validate risk ratings (Low to High scale)
    3. Clean investment objective text
    4. Map to internal fund codes (join key)

DEPENDENCIES:
    - Source: landing.vw_fund_metadata (from GCS CSV)
    
=============================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('landing', 'vw_fund_metadata') }}
),

cleaned AS (
    SELECT

        UPPER(TRIM(fund_code)) AS fund_code,
        TRIM(fund_name) AS fund_name,
        
        INITCAP(TRIM(fund_category)) AS fund_category,
        inception_date,
        TRIM(manager_name) AS manager_name,
        
        TRIM(investment_objective) AS investment_objective,
        minimum_investment,
        TRIM(dividend_frequency) AS dividend_frequency,
        
        INITCAP(TRIM(risk_rating)) AS risk_rating,
        TRIM(tax_treatment) AS tax_treatment,
        
        source_file_name,
        source_file_row_number,
        file_content_key,
        file_last_modified_timestamp,
        load_timestamp,
        load_source,
        
        CASE risk_rating
            WHEN 'Low' THEN 1
            WHEN 'Low-Medium' THEN 2
            WHEN 'Medium' THEN 3
            WHEN 'Medium-High' THEN 4
            WHEN 'High' THEN 5
            ELSE NULL
        END AS risk_score,
        
        CASE
            WHEN fund_code IS NULL THEN 'Missing Fund Code'
            WHEN investment_objective IS NULL THEN 'Missing Objective'
            WHEN risk_rating NOT IN ('Low', 'Low-Medium', 'Medium', 'Medium-High', 'High') 
                THEN 'Invalid Risk Rating'
            ELSE 'Valid'
        END AS data_quality_flag

    FROM source
    WHERE 1=1
        AND fund_code IS NOT NULL
        AND fund_name IS NOT NULL
)

SELECT * FROM cleaned