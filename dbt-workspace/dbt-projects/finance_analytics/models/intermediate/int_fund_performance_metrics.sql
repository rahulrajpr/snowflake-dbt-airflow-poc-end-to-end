{{
    config(
        materialized='table',
        tags=['intermediate', 'analytics']
    )
}}

/*
=============================================================================
MODEL: int_fund_performance_metrics
=============================================================================
PURPOSE:
    - Calculate fund performance metrics over various time periods
    - Compare against benchmarks
    - Foundation for fund ranking and selection

BUSINESS CONTEXT:
    - Funds evaluated on returns, volatility, and risk-adjusted returns
    - Performance metrics used for fund recommendations
    - Manager performance attribution

DEPENDENCIES:
    - int_daily_fund_nav
    
OUTPUT: FINANCE_DB.INTERMEDIATE.INT_FUND_PERFORMANCE_METRICS
=============================================================================
*/

WITH daily_nav AS (
    SELECT * FROM {{ ref('int_daily_fund_nav') }}
),

performance_calc AS (
    SELECT
        fund_id,
        fund_code,
        fund_name,
        fund_category,
        manager_id,
        manager_name,
        
        -- Latest metrics
        MAX(nav_date) AS latest_date,
        MAX(CASE WHEN nav_date = (SELECT MAX(nav_date) FROM daily_nav dn2 WHERE dn2.fund_code = daily_nav.fund_code) 
            THEN nav END) AS current_nav,
        MAX(CASE WHEN nav_date = (SELECT MAX(nav_date) FROM daily_nav dn2 WHERE dn2.fund_code = daily_nav.fund_code) 
            THEN aum_millions END) AS current_aum_millions,
        
        -- Returns (last available day)
        AVG(CASE WHEN nav_date >= CURRENT_DATE() - 7 THEN nav_change_pct END) AS return_1week_pct,
        AVG(CASE WHEN nav_date >= CURRENT_DATE() - 30 THEN nav_change_pct END) AS return_1month_pct,
        AVG(CASE WHEN nav_date >= CURRENT_DATE() - 90 THEN nav_change_pct END) AS return_3month_pct,
        AVG(CASE WHEN nav_date >= CURRENT_DATE() - 365 THEN nav_change_pct END) AS return_1year_pct,
        
        -- Volatility (standard deviation of returns)
        STDDEV(CASE WHEN nav_date >= CURRENT_DATE() - 30 THEN nav_change_pct END) AS volatility_1month,
        STDDEV(CASE WHEN nav_date >= CURRENT_DATE() - 365 THEN nav_change_pct END) AS volatility_1year,
        
        -- Benchmark comparison
        AVG(CASE WHEN nav_date >= CURRENT_DATE() - 30 THEN excess_return_pct END) AS excess_return_1month_pct,
        AVG(CASE WHEN nav_date >= CURRENT_DATE() - 365 THEN excess_return_pct END) AS excess_return_1year_pct,
        
        -- Sharpe Ratio approximation (return / volatility)
        CASE 
            WHEN STDDEV(CASE WHEN nav_date >= CURRENT_DATE() - 365 THEN nav_change_pct END) > 0
            THEN AVG(CASE WHEN nav_date >= CURRENT_DATE() - 365 THEN nav_change_pct END) / 
                 STDDEV(CASE WHEN nav_date >= CURRENT_DATE() - 365 THEN nav_change_pct END)
            ELSE 0
        END AS sharpe_ratio_1year,
        
        -- Audit
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM daily_nav
    GROUP BY 
        fund_id, fund_code, fund_name, fund_category, 
        manager_id, manager_name
)

SELECT * FROM performance_calc