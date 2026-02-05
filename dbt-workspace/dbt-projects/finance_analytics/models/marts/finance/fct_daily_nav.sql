{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'fact']
    )
}}

/*
=============================================================================
MODEL: fct_daily_nav
=============================================================================
PURPOSE:
    - Daily NAV fact table
    - Daily fund prices and performance metrics
    - Foundation for fund performance analytics

GRAIN: One row per fund per day

DEPENDENCIES:
    - int_daily_fund_nav
    - dim_funds
    - dim_dates
    
OUTPUT: FINANCE_DB.ANALYTICS.FCT_DAILY_NAV
=============================================================================
*/

WITH daily_nav AS (
    SELECT * FROM {{ ref('int_daily_fund_nav') }}
),

funds AS (
    SELECT fund_key, fund_id FROM {{ ref('dim_funds') }}
),

dates AS (
    SELECT date_key, date FROM {{ ref('dim_dates') }}
),

nav_fact AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['n.fund_id', 'n.nav_date']) }} AS daily_nav_key,
        
        -- Foreign Keys
        f.fund_key,
        d.date_key AS nav_date_key,
        
        -- Date
        n.nav_date,
        
        -- Measures
        n.nav,
        n.nav_change_pct,
        n.aum_millions,
        n.nav_7day_ma,
        n.nav_30day_ma,
        
        -- Benchmark
        n.benchmark_name,
        n.benchmark_change_pct,
        n.excess_return_pct,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM daily_nav n
    INNER JOIN funds f ON n.fund_id = f.fund_id
    INNER JOIN dates d ON n.nav_date = d.date
)

SELECT * FROM nav_fact