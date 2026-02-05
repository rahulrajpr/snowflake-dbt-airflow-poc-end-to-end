{{
    config(
        materialized='table',
        tags=['marts', 'analytics', 'aggregate']
    )
}}

/*
=============================================================================
MODEL: agg_weekly_investor_activity
=============================================================================
PURPOSE:
    - Weekly investor transaction summary
    - Pre-aggregated for Investor Analytics Dashboard
    - Track deposits vs withdrawals by segment

GRAIN: One row per investor per week

DEPENDENCIES:
    - fct_transactions
    - dim_investors
    - dim_dates
    
OUTPUT: FINANCE_DB.ANALYTICS.AGG_WEEKLY_INVESTOR_ACTIVITY
=============================================================================
*/

WITH transactions AS (
    SELECT * FROM {{ ref('fct_transactions') }}
),

investors AS (
    SELECT * FROM {{ ref('dim_investors') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
),

weekly_activity AS (
    SELECT
        -- Time Dimensions
        d.year,
        d.week_of_year,
        MIN(d.date) AS week_start_date,
        MAX(d.date) AS week_end_date,
        
        -- Investor Dimensions
        i.investor_key,
        i.investor_id,
        i.investor_name,
        i.customer_segment,
        i.risk_profile,
        i.city,
        i.state,
        i.country,
        
        -- Activity Metrics
        COUNT(DISTINCT t.transaction_id) AS transaction_count,
        COUNT(DISTINCT CASE WHEN t.transaction_type = 'BUY' THEN t.transaction_id END) AS buy_count,
        COUNT(DISTINCT CASE WHEN t.transaction_type = 'SELL' THEN t.transaction_id END) AS sell_count,
        COUNT(DISTINCT t.account_id) AS active_accounts,
        COUNT(DISTINCT t.fund_key) AS funds_transacted,
        
        -- Amount Metrics
        SUM(t.buy_amount) AS total_deposits,
        SUM(t.sell_amount) AS total_withdrawals,
        SUM(t.net_amount) AS net_flow,
        SUM(t.fees) AS total_fees_paid,
        
        -- Derived Metrics
        CASE 
            WHEN SUM(t.net_amount) > 0 THEN 'Net Depositor'
            WHEN SUM(t.net_amount) < 0 THEN 'Net Withdrawer'
            ELSE 'Neutral'
        END AS investor_flow_type,
        
        AVG(t.amount) AS avg_transaction_size,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM transactions t
    INNER JOIN investors i ON t.investor_key = i.investor_key
    INNER JOIN dates d ON t.transaction_date_key = d.date_key
    WHERE t.transaction_status = 'COMPLETED'
    GROUP BY 
        d.year, d.week_of_year,
        i.investor_key, i.investor_id, i.investor_name, 
        i.customer_segment, i.risk_profile, 
        i.city, i.state, i.country
)

SELECT * FROM weekly_activity