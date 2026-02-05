{{
    config(
        materialized='table',
        tags=['marts', 'analytics', 'aggregate']
    )
}}

/*
=============================================================================
MODEL: agg_daily_fund_performance
=============================================================================
PURPOSE:
    - Daily fund performance summary
    - Pre-aggregated for Fund Performance Dashboard
    - Key metrics: NAV change, AUM, transaction volume

GRAIN: One row per fund per day

DEPENDENCIES:
    - fct_daily_nav
    - fct_transactions
    - dim_funds
    - dim_dates
    
OUTPUT: FINANCE_DB.ANALYTICS.AGG_DAILY_FUND_PERFORMANCE
=============================================================================
*/

WITH daily_nav AS (
    SELECT * FROM {{ ref('fct_daily_nav') }}
),

daily_transactions AS (
    SELECT
        fund_key,
        transaction_date AS date,
        COUNT(*) AS transaction_count,
        SUM(buy_amount) AS total_buy_amount,
        SUM(sell_amount) AS total_sell_amount,
        SUM(net_amount) AS net_transaction_amount
    FROM {{ ref('fct_transactions') }}
    WHERE transaction_status = 'COMPLETED'
    GROUP BY fund_key, transaction_date
),

funds AS (
    SELECT * FROM {{ ref('dim_funds') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
),

daily_performance AS (
    SELECT
        -- Dimensions
        d.date,
        d.year,
        d.month,
        d.quarter,
        d.month_name,
        d.is_weekday,
        
        f.fund_key,
        f.fund_id,
        f.fund_code,
        f.fund_name,
        f.fund_category,
        f.manager_name,
        
        -- NAV Metrics
        n.nav,
        n.nav_change_pct,
        n.aum_millions,
        n.benchmark_change_pct,
        n.excess_return_pct,
        
        -- Transaction Metrics
        COALESCE(t.transaction_count, 0) AS transaction_count,
        COALESCE(t.total_buy_amount, 0) AS total_buy_amount,
        COALESCE(t.total_sell_amount, 0) AS total_sell_amount,
        COALESCE(t.net_transaction_amount, 0) AS net_transaction_amount,
        
        -- Derived Metrics
        CASE 
            WHEN n.nav_change_pct > 0 THEN 'Positive'
            WHEN n.nav_change_pct < 0 THEN 'Negative'
            ELSE 'Flat'
        END AS performance_direction,
        
        CASE 
            WHEN n.excess_return_pct > 0 THEN 'Outperforming'
            WHEN n.excess_return_pct < 0 THEN 'Underperforming'
            ELSE 'In-line'
        END AS vs_benchmark,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM daily_nav n
    INNER JOIN funds f ON n.fund_key = f.fund_key
    INNER JOIN dates d ON n.nav_date_key = d.date_key
    LEFT JOIN daily_transactions t ON n.fund_key = t.fund_key AND n.nav_date = t.date
)

SELECT * FROM daily_performance