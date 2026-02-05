{{
    config(
        materialized='table',
        tags=['intermediate', 'time_series']
    )
}}

/*
=============================================================================
MODEL: int_account_daily_balances
=============================================================================
PURPOSE:
    - Daily account balances with running totals
    - Calculate cumulative flows and returns
    - Foundation for investor portfolio analysis

BUSINESS CONTEXT:
    - Track daily account value changes
    - Separate investment returns from deposits/withdrawals
    - Calculate investor-level AUM

DEPENDENCIES:
    - stg_account_balances
    - int_investor_accounts
    
OUTPUT: FINANCE_DB.INTERMEDIATE.INT_ACCOUNT_DAILY_BALANCES
=============================================================================
*/

WITH balances AS (
    SELECT * FROM {{ ref('stg_account_balances') }}
),

accounts AS (
    SELECT * FROM {{ ref('int_investor_accounts') }}
),

balance_metrics AS (
    SELECT
        balance_date,
        account_id,
        opening_balance,
        closing_balance,
        net_deposits,
        net_withdrawals,
        net_investment_gain,
        net_flow,
        daily_balance_change,
        daily_change_pct,
        
        -- Cumulative metrics
        SUM(net_deposits) OVER (
            PARTITION BY account_id 
            ORDER BY balance_date
        ) AS cumulative_deposits,
        
        SUM(net_withdrawals) OVER (
            PARTITION BY account_id 
            ORDER BY balance_date
        ) AS cumulative_withdrawals,
        
        SUM(net_investment_gain) OVER (
            PARTITION BY account_id 
            ORDER BY balance_date
        ) AS cumulative_investment_gain,
        
        -- Running total return
        CASE 
            WHEN cumulative_deposits > 0 
            THEN (cumulative_investment_gain / cumulative_deposits) * 100
            ELSE 0
        END AS total_return_pct

    FROM balances
),

enriched AS (
    SELECT
        -- Date
        b.balance_date,
        
        -- Investor context
        a.investor_id,
        a.investor_name,
        a.customer_segment,
        a.risk_profile,
        
        -- Account context
        b.account_id,
        a.account_number,
        a.account_type,
        a.currency,
        
        -- Balance metrics
        b.opening_balance,
        b.closing_balance,
        b.daily_balance_change,
        b.daily_change_pct,
        
        -- Flow metrics
        b.net_deposits,
        b.net_withdrawals,
        b.net_flow,
        b.net_investment_gain,
        
        -- Cumulative metrics
        b.cumulative_deposits,
        b.cumulative_withdrawals,
        b.cumulative_investment_gain,
        b.total_return_pct,
        
        -- Audit
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM balance_metrics b
    INNER JOIN accounts a
        ON b.account_id = a.account_id
)

SELECT * FROM enriched