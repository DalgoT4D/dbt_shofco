{{ config(
    materialized='table',
    alias='sl_swep_products_produced_monthly_summary',
    tags=['sl', 'swep', 'monthly_summary', 'revenue', 'production']
) }}

WITH base AS (
    SELECT
        product_made,
        date_trunc('month', date_of_production::date) AS production_month,
        
        SUM(number_of_pieces_produced::numeric) AS total_pieces_produced,
        SUM(revenue::numeric) AS total_recorded_revenue,
        SUM(amount_per_piece::numeric * number_of_pieces_produced::numeric) AS total_calculated_revenue
    FROM {{ ref('staging_sl_swep_products_produced') }}
    GROUP BY product_made, production_month
)

SELECT 
    *,
    CASE 
        WHEN total_recorded_revenue = total_calculated_revenue THEN 'accurate'
        ELSE 'discrepancy'
    END AS revenue_consistency_status
FROM base
