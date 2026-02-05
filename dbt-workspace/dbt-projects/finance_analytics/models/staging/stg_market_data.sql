{{
    config(
        materialized='table',
        tags=['staging', 'gcs', 'benchmark']
    )
}}

/*
=============================================================================

PURPOSE:
    - Clean market benchmark index data from GCS (JSON source)
    - Provide comparison benchmarks for fund performance
    - Time series of major indices (S&P 500, NASDAQ, etc.)
    - Foundation for relative performance analysis

BUSINESS CONTEXT:
    - Market indices used as performance benchmarks
    - Funds compared against relevant index (e.g., equity fund vs S&P 500)
    - Daily index values for performance attribution
    - Used in fund factsheets and reporting

TRANSFORMATION LOGIC:
    1. Flatten JSON structure (already done by dynamic table)
    2. Standardize index names
    3. Calculate index absolute change
    4. Validate index values (positive, reasonable changes)

DEPENDENCIES:
    - Source: landing.vw_market_data (from GCS JSON, flattened by dynamic table)
    
=============================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('landing', 'vw_market_data') }}
),

cleaned AS (
    SELECT
        date AS market_date,
        
        TRIM(UPPER(index_name)) AS index_name,
        TRIM(UPPER(ticker)) AS ticker,
        
        value AS index_value,
        change_percent,
        
        value * (change_percent / 100) AS index_absolute_change,
        
        TRIM(note) AS note,
        
        source_file_name,
        source_file_row_number,
        file_content_key,
        file_last_modified_timestamp,
        load_timestamp,
        load_source,
        
        CASE
            WHEN value <= 0 THEN 'Invalid Index Value'
            WHEN ABS(change_percent) > 15 THEN 'Unusual Market Movement'
            WHEN market_date > CURRENT_DATE() THEN 'Future Date'
            ELSE 'Valid'
        END AS data_quality_flag

    FROM source
    
    WHERE 1=1
        AND date IS NOT NULL
        AND index_name IS NOT NULL
        -- AND value > 0
        AND date <= CURRENT_DATE()
)

SELECT * FROM cleaned