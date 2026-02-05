{{
    config(
        materialized='table',
        tags=['staging', 'postgres', 'high_volume']
    )
}}

/*
=============================================================================

PURPOSE:
    - Clean and validate all investment transactions
    - Calculate net transaction amounts (after fees/taxes)
    - Standardize transaction types and statuses
    - Foundation for transaction fact table and investor activity analysis

BUSINESS CONTEXT:
    - Captures all buy/sell/transfer activities
    - High volume table (50,000+ rows, grows daily)
    - Critical for AUM calculations and investor behavior analytics
    - NOTE: Will be converted to incremental in Phase 3 optimization

TRANSFORMATION LOGIC:
    1. Validate transaction amounts (must be positive for BUY, negative for SELL)
    2. Calculate transaction fees as percentage
    3. Ensure settlement date >= transaction date
    4. Filter cancelled transactions (business rule)

DEPENDENCIES:
    - Source: landing.vw_transactions
    
=============================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('landing', 'vw_transactions') }}
),

cleaned AS (
    SELECT
        transaction_id,
        UPPER(TRIM(transaction_number)) AS transaction_number,
        
        account_id,
        fund_id,

        UPPER(TRIM(transaction_type)) AS transaction_type,
        transaction_date,
        settlement_date,
        UPPER(TRIM(transaction_status)) AS transaction_status,
        UPPER(TRIM(channel)) AS channel,
        
        units,
        price_per_unit,
        amount,
        COALESCE(fees, 0) AS fees,
        COALESCE(tax_amount, 0) AS tax_amount,
        net_amount,
        
        CASE 
            WHEN amount != 0 THEN (fees / amount) * 100
            ELSE 0
        END AS fee_percentage,
        
        DATEDIFF(DAY, transaction_date, settlement_date) AS settlement_lag_days,
        notes,
        
        created_at,
        updated_at,
        load_timestamp,
        
        CASE
            WHEN transaction_type = 'BUY' AND amount <= 0 THEN 'Invalid Buy Amount'
            WHEN transaction_type = 'SELL' AND amount >= 0 THEN 'Invalid Sell Amount'
            WHEN settlement_date < transaction_date THEN 'Settlement Before Transaction'
            WHEN net_amount != (amount - fees - tax_amount) THEN 'Amount Mismatch'
            ELSE 'Valid'
        END AS data_quality_flag

    FROM source
    WHERE 1=1
        AND transaction_id IS NOT NULL
        AND transaction_number IS NOT NULL
        AND account_id IS NOT NULL
        AND fund_id IS NOT NULL
        AND transaction_status != 'CANCELLED'
        AND transaction_date IS NOT NULL
        AND amount IS NOT NULL
)

SELECT * FROM cleaned