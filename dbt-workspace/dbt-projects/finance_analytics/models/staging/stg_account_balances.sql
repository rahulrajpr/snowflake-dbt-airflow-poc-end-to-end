{{
    config(
        materialized='table',
        tags=['staging', 'postgres', 'high_volume', 'time_series']
    )
}}

/*
=============================================================================

PURPOSE:
    - Clean daily account balance snapshots
    - Calculate daily balance changes and net flows
    - Foundation for balance trend analysis and AUM time series
    - Largest table (244k+ rows) - will be optimized with incremental later

BUSINESS CONTEXT:
    - Daily snapshot of account balances
    - Used for AUM trend reporting and investor flow analysis
    - Critical for understanding investment vs. market performance
    - Net deposits = new money in, Net withdrawals = money out

TRANSFORMATION LOGIC:
    1. Validate balance arithmetic (opening + flows = closing)
    2. Calculate daily balance change percentage
    3. Identify unusual balance movements (spikes/drops)
    4. Handle missing dates in time series

DEPENDENCIES:
    - Source: landing.vw_account_balances
    
=============================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('landing', 'vw_account_balances') }}
),

cleaned AS (
    SELECT
        balance_id,
        account_id,
    
        balance_date,
        
        opening_balance,
        closing_balance,
        net_deposits,
        net_withdrawals,
        net_investment_gain,
        
        closing_balance - opening_balance AS daily_balance_change,
        
        CASE 
            WHEN opening_balance != 0 
            THEN ((closing_balance - opening_balance) / opening_balance) * 100
            ELSE 0
        END AS daily_change_pct,
        
        net_deposits - net_withdrawals AS net_flow,
        
        created_at,
        updated_at,
        load_timestamp,
        
        CASE
            WHEN ABS(closing_balance - (opening_balance + net_deposits - net_withdrawals + net_investment_gain)) > 0.01 
                THEN 'Balance Arithmetic Error'
            WHEN opening_balance < 0 THEN 'Negative Opening Balance'
            WHEN closing_balance < 0 THEN 'Negative Closing Balance'
            WHEN ABS(daily_change_pct) > 50 THEN 'Unusual Movement'
            ELSE 'Valid'
        END AS data_quality_flag

    FROM source
    
    WHERE 1=1
        AND balance_id IS NOT NULL
        AND account_id IS NOT NULL
        AND balance_date IS NOT NULL
        AND balance_date <= CURRENT_DATE()
)

SELECT * FROM cleaned