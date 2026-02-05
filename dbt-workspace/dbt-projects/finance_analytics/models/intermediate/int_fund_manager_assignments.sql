{{
    config(
        materialized='table',
        tags=['intermediate', 'fund_entity']
    )
}}

/*
=============================================================================
MODEL: int_fund_manager_assignments
=============================================================================
PURPOSE:
    - Join funds with managers and external metadata
    - Enrich fund data with ratings and risk scores
    - Foundation for fund performance analysis

BUSINESS CONTEXT:
    - Each fund has one manager
    - External metadata provides risk ratings and investment objectives
    - Ratings data provides quality scores

DEPENDENCIES:
    - stg_funds
    - stg_fund_managers
    - stg_fund_metadata
    - stg_ratings_data (optional - latest rating)
    
OUTPUT: FINANCE_DB.INTERMEDIATE.INT_FUND_MANAGER_ASSIGNMENTS
=============================================================================
*/

WITH funds AS (
    SELECT * FROM {{ ref('stg_funds') }}
),

managers AS (
    SELECT * FROM {{ ref('stg_fund_managers') }}
),

metadata AS (
    SELECT * FROM {{ ref('stg_fund_metadata') }}
),

latest_ratings AS (
    SELECT
        fund_code,
        overall_rating,
        composite_score,
        analyst_outlook,
        ROW_NUMBER() OVER (PARTITION BY fund_code ORDER BY report_date DESC) AS rn
    FROM {{ ref('stg_ratings_data') }}
),

joined AS (
    SELECT
        -- Fund attributes
        f.fund_id,
        f.fund_code,
        f.fund_name,
        f.fund_category,
        f.fund_type,
        f.inception_date,
        f.fund_age_years,
        f.expense_ratio,
        f.expense_ratio_bps,
        f.minimum_investment,
        f.aum_millions,
        f.benchmark_index,
        
        -- Manager attributes
        m.manager_id,
        m.manager_code,
        m.full_name AS manager_name,
        m.years_experience AS manager_experience,
        m.tenure_years AS manager_tenure,
        m.experience_level,
        m.specialization AS manager_specialization,
        
        -- External metadata
        meta.investment_objective,
        meta.risk_rating,
        meta.risk_score,
        meta.dividend_frequency,
        meta.tax_treatment,
        
        -- Latest ratings
        r.overall_rating,
        r.composite_score,
        r.analyst_outlook,
        
        -- Audit
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM funds f
    LEFT JOIN managers m
        ON f.manager_id = m.manager_id
    LEFT JOIN metadata meta
        ON f.fund_code = meta.fund_code
    LEFT JOIN latest_ratings r
        ON f.fund_code = r.fund_code
        AND r.rn = 1
)

SELECT * FROM joined