{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'fact']
    )
}}

/*
=============================================================================
MODEL: fct_transactions
=============================================================================
PURPOSE:
    - Transaction fact table
    - All investment transactions with dimensional context
    - Foundation for transaction analytics

GRAIN: One row per transaction

DEPENDENCIES:
    - int_transaction_enriched
    - dim_investors
    - dim_funds
    - dim_dates
    
OUTPUT: FINANCE_DB.ANALYTICS.FCT_TRANSACTIONS
=============================================================================
*/

WITH transactions AS (
    SELECT * FROM {{ ref('int_transaction_enriched') }}
),

investors AS (
    SELECT investor_key, investor_id FROM {{ ref('dim_investors') }}
),

funds AS (
    SELECT fund_key, fund_id FROM {{ ref('dim_funds') }}
),

dates AS (
    SELECT date_key, date FROM {{ ref('dim_dates') }}
),

transaction_fact AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['t.transaction_id']) }} AS transaction_key,
        
        -- Natural Key
        t.transaction_id,
        t.transaction_number,
        
        -- Foreign Keys (Dimension Keys)
        i.investor_key,
        f.fund_key,
        d.date_key AS transaction_date_key,
        
        -- Degenerate Dimensions
        t.account_id,
        t.account_number,
        t.account_type,
        
        -- Transaction Attributes
        t.transaction_type,
        t.transaction_date,
        t.settlement_date,
        t.settlement_lag_days,
        t.transaction_status,
        t.channel,
        
        -- Measures (Additive)
        t.units,
        t.price_per_unit,
        t.amount,
        t.fees,
        t.tax_amount,
        t.net_amount,
        t.buy_amount,
        t.sell_amount,
        
        -- Measures (Semi-Additive)
        t.fee_percentage,
        
        -- Metadata
        t.created_at AS transaction_created_at,
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM transactions t
    INNER JOIN investors i ON t.investor_id = i.investor_id
    INNER JOIN funds f ON t.fund_id = f.fund_id
    INNER JOIN dates d ON t.transaction_date = d.date
)

SELECT * FROM transaction_fact