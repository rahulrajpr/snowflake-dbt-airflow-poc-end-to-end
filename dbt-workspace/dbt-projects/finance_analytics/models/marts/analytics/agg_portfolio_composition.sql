{{
    config(
        materialized='table',
        tags=['marts', 'analytics', 'aggregate']
    )
}}

/*
=============================================================================
MODEL: agg_portfolio_composition
=============================================================================
PURPOSE:
    - Current portfolio composition by investor
    - Pre-aggregated for Portfolio Dashboard
    - Holdings breakdown by fund category

GRAIN: One row per investor per fund category

DEPENDENCIES:
    - stg_fund_holdings
    - int_investor_accounts
    - dim_funds
    
OUTPUT: FINANCE_DB.ANALYTICS.AGG_PORTFOLIO_COMPOSITION
=============================================================================
*/

WITH holdings AS (
    SELECT * FROM {{ ref('stg_fund_holdings') }}
),

investor_accounts AS (
    SELECT 
        investor_id,
        investor_name,
        customer_segment,
        risk_profile,
        account_id
    FROM {{ ref('int_investor_accounts') }}
),

funds AS (
    SELECT 
        fund_id,
        fund_category
    FROM {{ ref('dim_funds') }}
),

investor_holdings AS (
    SELECT
        ia.investor_id,
        ia.investor_name,
        ia.customer_segment,
        ia.risk_profile,
        
        f.fund_category,
        
        COUNT(DISTINCT h.holding_id) AS holdings_count,
        COUNT(DISTINCT h.fund_id) AS funds_in_category,
        SUM(h.current_value) AS total_value,
        SUM(h.cost_basis) AS total_cost_basis,
        SUM(h.unrealized_gain_loss) AS total_unrealized_gain_loss,
        AVG(h.unrealized_return_pct) AS avg_unrealized_return_pct,
        
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM holdings h
    INNER JOIN investor_accounts ia 
        ON h.account_id = ia.account_id
    INNER JOIN funds f 
        ON h.fund_id = f.fund_id
    GROUP BY 
        ia.investor_id, 
        ia.investor_name, 
        ia.customer_segment, 
        ia.risk_profile, 
        f.fund_category
),

portfolio_totals AS (
    SELECT
        investor_id,
        SUM(total_value) AS portfolio_total_value
    FROM investor_holdings
    GROUP BY investor_id
),

portfolio_composition AS (
    SELECT
        h.*,
        pt.portfolio_total_value,
        (h.total_value / NULLIF(pt.portfolio_total_value, 0)) * 100 AS pct_of_portfolio,
        
        CASE 
            WHEN h.total_unrealized_gain_loss > 0 THEN 'Profitable'
            WHEN h.total_unrealized_gain_loss < 0 THEN 'Loss'
            ELSE 'Break-even'
        END AS performance_status

    FROM investor_holdings h
    INNER JOIN portfolio_totals pt 
        ON h.investor_id = pt.investor_id
)

SELECT * FROM portfolio_compositionana