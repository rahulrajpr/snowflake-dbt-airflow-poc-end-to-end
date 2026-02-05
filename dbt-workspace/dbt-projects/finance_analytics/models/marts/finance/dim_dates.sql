{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension']
    )
}}

/*
=============================================================================
MODEL: dim_dates
=============================================================================
PURPOSE:
    - Date dimension table
    - Calendar attributes for time-based analysis
    - Foundation for time series queries

GRAIN: One row per date

DEPENDENCIES:
    - None (generated using dbt_utils)
    
OUTPUT: FINANCE_DB.ANALYTICS.DIM_DATES
=============================================================================
*/

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

date_dim AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
        
        -- Natural Key
        date_day AS date,
        
        -- Date Parts
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        MONTH(date_day) AS month,
        WEEK(date_day) AS week_of_year,
        DAY(date_day) AS day_of_month,
        DAYOFWEEK(date_day) AS day_of_week,
        DAYOFYEAR(date_day) AS day_of_year,
        
        -- Date Names
        TO_CHAR(date_day, 'MMMM') AS month_name,
        TO_CHAR(date_day, 'Mon') AS month_name_short,
        TO_CHAR(date_day, 'Day') AS day_name,
        TO_CHAR(date_day, 'Dy') AS day_name_short,
        
        -- Flags
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN DAYOFWEEK(date_day) BETWEEN 1 AND 5 THEN TRUE ELSE FALSE END AS is_weekday,
        
        -- Fiscal (assuming Jan 1 start)
        YEAR(date_day) AS fiscal_year,
        QUARTER(date_day) AS fiscal_quarter,
        
        -- Relative Dates
        CASE WHEN date_day = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_today,
        CASE WHEN date_day = CURRENT_DATE() - 1 THEN TRUE ELSE FALSE END AS is_yesterday,
        CASE WHEN date_day >= CURRENT_DATE() - 7 THEN TRUE ELSE FALSE END AS is_last_7_days,
        CASE WHEN date_day >= CURRENT_DATE() - 30 THEN TRUE ELSE FALSE END AS is_last_30_days,
        CASE WHEN date_day >= CURRENT_DATE() - 90 THEN TRUE ELSE FALSE END AS is_last_90_days,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM date_spine
)

SELECT * FROM date_dim