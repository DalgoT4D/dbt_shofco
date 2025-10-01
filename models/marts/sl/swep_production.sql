{{
    config(
        materialized='table',
        tags=['swep', 'swep_production', 'sl', "sl_marts"]
    )
}}

SELECT
    id AS form_id,
    case_id,
    {{ validate_date('date_of_production_sp') }} AS date_of_production,
    CASE WHEN unique_items_produced_sp ~ '^[0-9]+$' THEN unique_items_produced_sp::INTEGER ELSE NULL END AS unique_items_produced,
    NULL AS fill_instructions,
    product_made_sp AS product_name,
    CASE WHEN revenue_sp ~ '^[0-9]+(\.[0-9]+)?$' THEN revenue_sp::NUMERIC ELSE NULL END AS revenue,
    CASE WHEN pieces_produced_sp ~ '^[0-9]+$' THEN pieces_produced_sp::INTEGER ELSE NULL END AS pieces_produced,
    CASE WHEN amount_per_piece_sp ~ '^[0-9]+(\.[0-9]+)?$' THEN amount_per_piece_sp::NUMERIC ELSE NULL END AS amount_per_piece
FROM {{ ref('staging_sl_case_table') }}
WHERE product_made_sp IS NOT NULL