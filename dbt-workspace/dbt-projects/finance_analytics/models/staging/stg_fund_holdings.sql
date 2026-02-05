{{
    config(
        materialized='table',
        tags=['staging', 'postgres']
    )
}}

/*
=============================================================================

PURPOSE:
    - Clean current fund holdings snapshot data
    - Calculate unrealized gains/losses
    - Validate holding positions (no negative units)
    - Foundation for portfolio composition and AUM analysis

BUSINESS CONTEXT:
    - Represents current positions (what investors own right now)
    - Used for portfolio rebalancing recommendations
    - Critical for accurate AUM reporting
    - Updated as transactions are processed

TRANSFORMATION LOGIC:
    1. Calculate unrealized P&L (current value vs. cost basis)
    2. Calculate return percentage on holdings
    3. Filter zero/negative holdings (data quality)
    4. Compute average cost per unit

DEPENDENCIES:
    - Source: landing.vw_fund_holdings

=============================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('landing', 'vw_fund_holdings') }}
),

cleaned AS (
    SELECT
        holding_id,
        account_id,
        fund_id,
        
        units_held,
        average_cost_per_unit,
        current_value,
        last_transaction_date,
        
        units_held * average_cost_per_unit AS cost_basis,
        current_value - (units_held * average_cost_per_unit) AS unrealized_gain_loss,
        
        CASE 
            WHEN (units_held * average_cost_per_unit) != 0 
            THEN ((current_value - (units_held * average_cost_per_unit)) / (units_held * average_cost_per_unit)) * 100
            ELSE 0
        END AS unrealized_return_pct,
        
        created_at,
        updated_at,
        load_timestamp,
        
        CASE
            WHEN units_held <= 0 THEN 'Invalid Units'
            WHEN current_value <= 0 THEN 'Invalid Value'
            WHEN average_cost_per_unit <= 0 THEN 'Invalid Cost Basis'
            ELSE 'Valid'
        END AS data_quality_flag

    FROM source
    
    WHERE 1=1
        AND holding_id IS NOT NULL
        AND account_id IS NOT NULL
        AND fund_id IS NOT NULL
        AND units_held > 0
        AND current_value > 0
)

SELECT * FROM cleaned