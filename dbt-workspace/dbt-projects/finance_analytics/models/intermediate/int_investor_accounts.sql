{{
    config(
        materialized='table',
        tags=['intermediate', 'core_entity']
    )
}}

/*
=============================================================================
MODEL: int_investor_accounts
=============================================================================
PURPOSE:
    - Join investors with their accounts
    - Enrich with account-level aggregates
    - Foundation for investor analytics

BUSINESS CONTEXT:
    - One investor can have multiple accounts
    - Need investor + account combined view for analysis

DEPENDENCIES:
    - stg_investors
    - stg_accounts
    
OUTPUT: FINANCE_DB.INTERMEDIATE.INT_INVESTOR_ACCOUNTS
=============================================================================
*/

WITH investors AS (
    SELECT * FROM {{ ref('stg_investors') }}
),

accounts AS (
    SELECT * FROM {{ ref('stg_accounts') }}
),

joined AS (
    SELECT
        -- Investor attributes
        i.investor_id,
        i.full_name AS investor_name,
        i.email,
        i.age,
        i.registration_date,
        i.kyc_status,
        i.risk_profile,
        i.customer_segment,
        i.city,
        i.state,
        i.country,
        
        -- Account attributes
        a.account_id,
        a.account_number,
        a.account_type,
        a.account_status,
        a.currency,
        a.opening_date,
        a.closing_date,
        a.total_balance,
        a.account_age_days,
        a.is_currently_active,
        
        -- Derived metrics
        DATEDIFF(DAY, i.registration_date, a.opening_date) AS days_to_first_account,
        
        -- Audit
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM investors i
    INNER JOIN accounts a
        ON i.investor_id = a.investor_id
)

SELECT * FROM joined