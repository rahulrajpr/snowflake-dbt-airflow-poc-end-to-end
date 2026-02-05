{{
    config(
        materialized='table',
        tags=['intermediate', 'time_series']
    )
}}

/*
=============================================================================
MODEL: int_daily_fund_nav
=============================================================================
PURPOSE:
    - Daily NAV with day-over-day changes
    - Join with market benchmarks for comparison
    - Calculate relative performance

BUSINESS CONTEXT:
    - NAV = daily fund price
    - Compare fund performance vs market benchmarks
    - Track AUM changes

DEPENDENCIES:
    - stg_nav_data
    - int_fund_manager_assignments
    - stg_market_data
    
OUTPUT: FINANCE_DB.INTERMEDIATE.INT_DAILY_FUND_NAV
=============================================================================
*/

WITH nav_data AS (
    SELECT * FROM {{ ref('stg_nav_data') }}
),

funds AS (
    SELECT * FROM {{ ref('int_fund_manager_assignments') }}
),

market_data AS (
    SELECT
        market_date,
        index_name,
        index_value,
        change_percent AS market_change_percent
    FROM {{ ref('stg_market_data') }}
),

nav_with_lag AS (
    SELECT
        nav_date,
        fund_code,
        nav,
        change_percent,
        total_assets_millions,
        
        -- Previous day NAV for validation
        LAG(nav) OVER (PARTITION BY fund_code ORDER BY nav_date) AS prev_nav,
        LAG(nav_date) OVER (PARTITION BY fund_code ORDER BY nav_date) AS prev_nav_date,
        
        -- Moving averages
        AVG(nav) OVER (
            PARTITION BY fund_code 
            ORDER BY nav_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS nav_7day_ma,
        
        AVG(nav) OVER (
            PARTITION BY fund_code 
            ORDER BY nav_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS nav_30day_ma

    FROM nav_data
),

enriched AS (
    SELECT
        -- Date
        n.nav_date,
        
        -- Fund details
        f.fund_id,
        n.fund_code,
        f.fund_name,
        f.fund_category,
        f.fund_type,
        f.benchmark_index,
        
        -- Manager
        f.manager_id,
        f.manager_name,
        
        -- NAV metrics
        n.nav,
        n.change_percent AS nav_change_pct,
        n.total_assets_millions AS aum_millions,
        n.nav_7day_ma,
        n.nav_30day_ma,
        
        -- Market benchmark
        m.index_name AS benchmark_name,
        m.market_change_percent AS benchmark_change_pct,
        
        -- Relative performance
        n.change_percent - COALESCE(m.market_change_percent, 0) AS excess_return_pct,
        
        -- Audit
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM nav_with_lag n
    INNER JOIN funds f
        ON n.fund_code = f.fund_code
    LEFT JOIN market_data m
        ON n.nav_date = m.market_date
        AND f.benchmark_index = m.index_name
)

SELECT * FROM enriched