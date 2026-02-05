{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'fact']
    )
}}

/*
=============================================================================
MODEL: fct_account_balances
=============================================================================
PURPOSE:
    - Daily account balance fact table
    - Track account values and flows over time
    - Foundation for investor portfolio analytics

GRAIN: One row per account per day

DEPENDENCIES:
    - int_account_daily_balances
    - dim_investors
    - dim_dates
    
OUTPUT: FINANCE_DB.ANALYTICS.FCT_ACCOUNT_BALANCES
=============================================================================
*/

WITH balances AS (
    SELECT * FROM {{ ref('int_account_daily_balances') }}
),

investors AS (
    SELECT investor_key, investor_id FROM {{ ref('dim_investors') }}
),

dates AS (
    SELECT date_key, date FROM {{ ref('dim_dates') }}
),

balance_fact AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['b.account_id', 'b.balance_date']) }} AS account_balance_key,
        
        -- Foreign Keys
        i.investor_key,
        d.date_key AS balance_date_key,
        
        -- Degenerate Dimensions
        b.account_id,
        b.account_number,
        b.account_type,
        b.currency,
        
        -- Date
        b.balance_date,
        
        -- Balance Measures
        b.opening_balance,
        b.closing_balance,
        b.daily_balance_change,
        b.daily_change_pct,
        
        -- Flow Measures
        b.net_deposits,
        b.net_withdrawals,
        b.net_flow,
        b.net_investment_gain,
        
        -- Cumulative Measures
        b.cumulative_deposits,
        b.cumulative_withdrawals,
        b.cumulative_investment_gain,
        b.total_return_pct,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM balances b
    INNER JOIN investors i ON b.investor_id = i.investor_id
    INNER JOIN dates d ON b.balance_date = d.date
)

SELECT * FROM balance_fact