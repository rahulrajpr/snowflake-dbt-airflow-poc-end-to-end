{{
    config(
        materialized='table',
        tags=['marts', 'analytics', 'aggregate']
    )
}}

/*
=============================================================================
MODEL: agg_fund_manager_performance
=============================================================================
PURPOSE:
    - Fund manager performance summary
    - Pre-aggregated for Manager Performance Dashboard
    - Compare managers across all their funds

GRAIN: One row per manager

DEPENDENCIES:
    - int_fund_performance_metrics
    - dim_fund_managers
    
OUTPUT: FINANCE_DB.ANALYTICS.AGG_FUND_MANAGER_PERFORMANCE
=============================================================================
*/

WITH performance AS (
    SELECT * FROM {{ ref('int_fund_performance_metrics') }}
),

managers AS (
    SELECT * FROM {{ ref('dim_fund_managers') }}
),

manager_performance AS (
    SELECT
        -- Manager Dimensions
        m.manager_key,
        m.manager_id,
        m.manager_name,
        m.years_experience,
        m.tenure_years,
        m.experience_level,
        m.specialization,
        
        -- Fund Count
        COUNT(DISTINCT p.fund_id) AS funds_managed,
        
        -- AUM
        SUM(p.current_aum_millions) AS total_aum_millions,
        AVG(p.current_aum_millions) AS avg_fund_aum_millions,
        
        -- Performance Metrics (Weighted by AUM)
        SUM(p.return_1month_pct * p.current_aum_millions) / NULLIF(SUM(p.current_aum_millions), 0) AS weighted_return_1month_pct,
        SUM(p.return_1year_pct * p.current_aum_millions) / NULLIF(SUM(p.current_aum_millions), 0) AS weighted_return_1year_pct,
        
        -- Average Performance (Unweighted)
        AVG(p.return_1month_pct) AS avg_return_1month_pct,
        AVG(p.return_1year_pct) AS avg_return_1year_pct,
        
        -- Best/Worst Fund
        MAX(p.return_1year_pct) AS best_fund_return_1year_pct,
        MIN(p.return_1year_pct) AS worst_fund_return_1year_pct,
        
        -- Risk Metrics
        AVG(p.volatility_1year) AS avg_volatility_1year,
        AVG(p.sharpe_ratio_1year) AS avg_sharpe_ratio_1year,
        
        -- Benchmark Comparison
        AVG(p.excess_return_1year_pct) AS avg_excess_return_1year_pct,
        
        -- Performance Classification
        CASE 
            WHEN AVG(p.return_1year_pct) > 10 THEN 'Top Performer'
            WHEN AVG(p.return_1year_pct) > 5 THEN 'Above Average'
            WHEN AVG(p.return_1year_pct) > 0 THEN 'Average'
            ELSE 'Below Average'
        END AS performance_tier,
        
        -- Metadata
        MAX(p.latest_date) AS as_of_date,
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM performance p
    INNER JOIN managers m ON p.manager_id = m.manager_id
    GROUP BY 
        m.manager_key, m.manager_id, m.manager_name, 
        m.years_experience, m.tenure_years, 
        m.experience_level, m.specialization
)

SELECT * FROM manager_performance