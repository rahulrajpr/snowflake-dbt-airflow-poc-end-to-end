{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension']
    )
}}

/*
=============================================================================
MODEL: dim_funds
=============================================================================
PURPOSE:
    - Fund dimension table (SCD Type 1)
    - Current fund attributes with manager and ratings
    - Foundation for fund performance analytics

GRAIN: One row per fund

DEPENDENCIES:
    - int_fund_manager_assignments
    
OUTPUT: FINANCE_DB.ANALYTICS.DIM_FUNDS
=============================================================================
*/

WITH funds AS (
    SELECT * FROM {{ ref('int_fund_manager_assignments') }}
),

fund_dim AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['fund_id']) }} AS fund_key,
        
        -- Natural Key
        fund_id,
        fund_code,
        
        -- Attributes
        fund_name,
        fund_category,
        fund_type,
        inception_date,
        fund_age_years,
        
        -- Costs
        expense_ratio,
        expense_ratio_bps,
        minimum_investment,
        
        -- Performance
        benchmark_index,
        
        -- Manager
        manager_id,
        manager_code,
        manager_name,
        manager_experience,
        manager_tenure,
        experience_level,
        manager_specialization,
        
        -- External Data
        investment_objective,
        risk_rating,
        risk_score,
        dividend_frequency,
        tax_treatment,
        
        -- Ratings
        overall_rating,
        composite_score,
        analyst_outlook,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM funds
)

SELECT * FROM fund_dim