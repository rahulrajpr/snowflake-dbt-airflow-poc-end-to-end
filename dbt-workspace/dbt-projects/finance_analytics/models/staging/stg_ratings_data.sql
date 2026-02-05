{{
    config(
        materialized='table',
        tags=['staging', 'gcs', 'external_ratings']
    )
}}

/*
=============================================================================

PURPOSE:
    - Clean fund ratings from external rating agencies (GCS JSON source)
    - Standardize rating scales across vendors
    - Foundation for fund quality scoring and recommendations
    - Track rating changes over time

BUSINESS CONTEXT:
    - Third-party fund ratings (like Morningstar)
    - Overall rating (1-5 stars) plus component scores
    - Risk score, return score, expense score
    - Analyst outlook (Positive/Neutral/Negative)
    - Used in investor-facing materials and fund selection

TRANSFORMATION LOGIC:
    1. Flatten JSON structure (already done by dynamic table)
    2. Validate rating scales (1-5 for overall, 0-100 for component scores)
    3. Standardize vendor names
    4. Calculate composite quality score

DEPENDENCIES:
    - Source: landing.vw_ratings_data (from GCS JSON, flattened by dynamic table)
    
=============================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('landing', 'vw_ratings_data') }}
),

cleaned AS (
    SELECT
        report_date,
        
        TRIM(UPPER(vendor)) AS vendor,
        
        UPPER(TRIM(fund_code)) AS fund_code,
        TRIM(fund_name) AS fund_name,
        
        overall_rating,
        
        risk_score,
        return_score,
        expense_score,
        
        category_rank,
        
        INITCAP(TRIM(analyst_outlook)) AS analyst_outlook,
        
        (return_score * 0.4 + (100 - risk_score) * 0.3 + (100 - expense_score) * 0.3) AS composite_score,
        
        source_file_name,
        source_file_row_number,
        file_content_key,
        file_last_modified_timestamp,
        load_timestamp,
        load_source,
        
        CASE
            WHEN overall_rating NOT BETWEEN 1 AND 5 THEN 'Invalid Overall Rating'
            WHEN risk_score NOT BETWEEN 0 AND 100 THEN 'Invalid Risk Score'
            WHEN return_score NOT BETWEEN 0 AND 100 THEN 'Invalid Return Score'
            WHEN expense_score NOT BETWEEN 0 AND 100 THEN 'Invalid Expense Score'
            WHEN analyst_outlook NOT IN ('Positive', 'Neutral', 'Negative', 'Under Review') 
                THEN 'Invalid Outlook'
            ELSE 'Valid'
        END AS data_quality_flag

    FROM source
    WHERE 1=1
        AND report_date IS NOT NULL
        AND vendor IS NOT NULL
        AND fund_code IS NOT NULL
        AND overall_rating BETWEEN 1 AND 5
)

SELECT * FROM cleaned