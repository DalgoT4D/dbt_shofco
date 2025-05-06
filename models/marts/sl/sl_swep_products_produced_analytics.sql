{{ config(
    materialized='table',
    schema='intermediate_sl',
    alias='sl_swep_products_produced_analytics',
    tags=['sl', 'swep', 'production', 'analytics']
) }}

WITH source AS (
    SELECT * FROM {{ ref('staging_sl_swep_products_produced') }}
),

enriched AS (
    SELECT
        case_id,
        case_name,
        date_opened,
        mpesa_name,
        mpesa_number,
        product_made,
        amount_per_piece::numeric AS amount_per_piece,
        number_of_pieces_produced::numeric AS number_of_pieces_produced,
        revenue::numeric AS recorded_revenue,
        date_of_production::date AS date_of_production,

        -- Calculate expected revenue
        (amount_per_piece::numeric * number_of_pieces_produced::numeric) AS calculated_revenue,

        -- Check for revenue consistency
        CASE
            WHEN revenue::numeric = (amount_per_piece::numeric * number_of_pieces_produced::numeric)
            THEN 'accurate'
            ELSE 'discrepancy'
        END AS revenue_status
    FROM source
)

SELECT * FROM enriched
