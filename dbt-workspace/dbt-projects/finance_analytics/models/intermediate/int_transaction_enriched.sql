{{
    config(
        materialized='table',
        tags=['intermediate', 'transaction_fact']
    )
}}

/*
=============================================================================
MODEL: int_transaction_enriched
=============================================================================
PURPOSE:
    - Enrich transactions with investor, account, and fund details
    - Calculate derived transaction metrics
    - Foundation for transaction analytics

BUSINESS CONTEXT:
    - Transactions are the core events
    - Need context from investors, accounts, and funds for analysis

DEPENDENCIES:
    - stg_transactions
    - int_investor_accounts
    - int_fund_manager_assignments
    
OUTPUT: FINANCE_DB.INTERMEDIATE.INT_TRANSACTION_ENRICHED
=============================================================================
*/

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

accounts AS (
    SELECT * FROM {{ ref('int_investor_accounts') }}
),

funds AS (
    SELECT * FROM {{ ref('int_fund_manager_assignments') }}
),

enriched AS (
    SELECT
        -- Transaction attributes
        t.transaction_id,
        t.transaction_number,
        t.transaction_type,
        t.transaction_date,
        t.settlement_date,
        t.settlement_lag_days,
        t.transaction_status,
        t.channel,
        
        -- Financial amounts
        t.units,
        t.price_per_unit,
        t.amount,
        t.fees,
        t.fee_percentage,
        t.tax_amount,
        t.net_amount,
        
        -- Investor context
        a.investor_id,
        a.investor_name,
        a.customer_segment,
        a.risk_profile,
        a.city,
        a.state,
        a.country,
        
        -- Account context
        t.account_id,
        a.account_number,
        a.account_type,
        a.currency,
        
        -- Fund context
        t.fund_id,
        f.fund_code,
        f.fund_name,
        f.fund_category,
        f.fund_type,
        f.expense_ratio_bps,
        
        -- Manager context
        f.manager_id,
        f.manager_name,
        
        -- Derived metrics
        CASE 
            WHEN t.transaction_type = 'BUY' THEN t.net_amount
            ELSE 0
        END AS buy_amount,
        
        CASE 
            WHEN t.transaction_type = 'SELL' THEN ABS(t.net_amount)
            ELSE 0
        END AS sell_amount,
        
        -- Audit
        t.created_at,
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM transactions t
    INNER JOIN accounts a
        ON t.account_id = a.account_id
    INNER JOIN funds f
        ON t.fund_id = f.fund_id
)

SELECT * FROM enriched