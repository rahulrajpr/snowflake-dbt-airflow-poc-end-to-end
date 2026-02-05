{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension', 'pii']
    )
}}

/*
=============================================================================
MODEL: dim_fund_managers
=============================================================================
PURPOSE:
    - Fund manager dimension table (SCD Type 1)
    - Manager attributes and experience
    - Foundation for manager performance attribution

GRAIN: One row per manager

DEPENDENCIES:
    - stg_fund_managers
    
OUTPUT: FINANCE_DB.ANALYTICS.DIM_FUND_MANAGERS
=============================================================================
*/

WITH managers AS (
    SELECT * FROM {{ ref('stg_fund_managers') }}
),

manager_dim AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['manager_id']) }} AS manager_key,
        
        -- Natural Key
        manager_id,
        manager_code,
        
        -- Attributes
        full_name AS manager_name,
        first_name,
        last_name,
        email,
        
        -- Professional Details
        years_experience,
        education,
        specialization,
        experience_level,
        
        -- Employment
        hire_date,
        tenure_years,
        is_active,
        
        -- Metadata
        created_at,
        updated_at,
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM managers
)

SELECT * FROM manager_dim