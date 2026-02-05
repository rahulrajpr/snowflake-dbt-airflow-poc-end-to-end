{{
    config(
        materialized='table',
        tags=['marts', 'analytics', 'aggregate']
    )
}}

/*
=============================================================================
MODEL: agg_customer_segmentation
=============================================================================
PURPOSE:
    - Customer segmentation by investment behavior
    - Pre-aggregated for Customer Analytics Dashboard
    - RFM-style analysis (Recency, Frequency, Monetary)

GRAIN: One row per investor

DEPENDENCIES:
    - fct_transactions
    - fct_account_balances
    - dim_investors
    
OUTPUT: FINANCE_DB.ANALYTICS.AGG_CUSTOMER_SEGMENTATION
=============================================================================
*/

WITH investors AS (
    SELECT * FROM {{ ref('dim_investors') }}
),

transaction_summary AS (
    SELECT
        investor_key,
        COUNT(*) AS total_transactions,
        SUM(net_amount) AS lifetime_value,
        MAX(transaction_date) AS last_transaction_date,
        MIN(transaction_date) AS first_transaction_date,
        AVG(net_amount) AS avg_transaction_value,
        COUNT(DISTINCT fund_key) AS funds_invested_in
    FROM {{ ref('fct_transactions') }}
    WHERE transaction_status = 'COMPLETED'
    GROUP BY investor_key
),

balance_summary AS (
    SELECT
        investor_key,
        MAX(closing_balance) AS current_balance,
        AVG(closing_balance) AS avg_balance,
        MAX(total_return_pct) AS total_return_pct
    FROM {{ ref('fct_account_balances') }}
    WHERE balance_date >= CURRENT_DATE() - 30
    GROUP BY investor_key
),

segmentation AS (
    SELECT
        -- Investor Dimensions
        i.investor_key,
        i.investor_id,
        i.investor_name,
        i.customer_segment,
        i.risk_profile,
        i.age,
        i.registration_date,
        i.city,
        i.state,
        i.country,
        
        -- Transaction Metrics
        COALESCE(t.total_transactions, 0) AS total_transactions,
        COALESCE(t.lifetime_value, 0) AS lifetime_value,
        t.last_transaction_date,
        t.first_transaction_date,
        COALESCE(t.avg_transaction_value, 0) AS avg_transaction_value,
        COALESCE(t.funds_invested_in, 0) AS funds_invested_in,
        
        -- Balance Metrics
        COALESCE(b.current_balance, 0) AS current_balance,
        COALESCE(b.avg_balance, 0) AS avg_balance,
        COALESCE(b.total_return_pct, 0) AS total_return_pct,
        
        -- RFM Scores
        DATEDIFF(DAY, t.last_transaction_date, CURRENT_DATE()) AS days_since_last_transaction,
        DATEDIFF(DAY, i.registration_date, CURRENT_DATE()) AS days_as_customer,
        
        -- Behavioral Classification
        CASE 
            WHEN t.total_transactions >= 50 THEN 'Very Active'
            WHEN t.total_transactions >= 20 THEN 'Active'
            WHEN t.total_transactions >= 5 THEN 'Moderate'
            ELSE 'Occasional'
        END AS activity_level,
        
        CASE 
            WHEN b.current_balance >= 100000 THEN 'High Value'
            WHEN b.current_balance >= 50000 THEN 'Medium Value'
            WHEN b.current_balance >= 10000 THEN 'Growing'
            ELSE 'Starter'
        END AS value_tier,
        
        CASE 
            WHEN DATEDIFF(DAY, t.last_transaction_date, CURRENT_DATE()) <= 30 THEN 'Active'
            WHEN DATEDIFF(DAY, t.last_transaction_date, CURRENT_DATE()) <= 90 THEN 'At Risk'
            WHEN DATEDIFF(DAY, t.last_transaction_date, CURRENT_DATE()) <= 180 THEN 'Dormant'
            ELSE 'Churned'
        END AS engagement_status,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM investors i
    LEFT JOIN transaction_summary t ON i.investor_key = t.investor_key
    LEFT JOIN balance_summary b ON i.investor_key = b.investor_key
)

SELECT * FROM segmentation