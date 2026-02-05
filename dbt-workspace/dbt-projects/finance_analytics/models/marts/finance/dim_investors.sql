{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension', 'pii']
    )
}}

/*
=============================================================================
MODEL: dim_investors
=============================================================================
PURPOSE:
    - Investor dimension table (SCD Type 1)
    - Current state of investor attributes
    - Foundation for investor analytics

GRAIN: One row per investor

DEPENDENCIES:
    - stg_investors
    
OUTPUT: FINANCE_DB.ANALYTICS.DIM_INVESTORS
=============================================================================
*/

WITH investors AS (
    SELECT * FROM {{ ref('stg_investors') }}
),

investor_dim AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['investor_id']) }} AS investor_key,
        
        -- Natural Key
        investor_id,
        
        -- Attributes
        full_name AS investor_name,
        first_name,
        last_name,
        email,
        phone,
        date_of_birth,
        age,
        registration_date,
        
        -- Classification
        kyc_status,
        risk_profile,
        customer_segment,
        
        -- Location
        address_line1,
        city,
        state,
        postal_code,
        country,
        
        -- Status
        is_active,
        
        -- Metadata
        created_at,
        updated_at,
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM investors
)

SELECT * FROM investor_dim