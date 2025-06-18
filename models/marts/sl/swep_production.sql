{{
    config(
        materialized='table',
        tags=['swep', 'swep_production', 'sl']
    )
}}

SELECT
    form_id,
    case_id,
    {{ validate_date('date_of_production_sp') }} AS date_of_production,
    NULLIF(unique_items_produced_sp, '')::INTEGER AS unique_items_produced,
    NULLIF(fill_instructions_sp, '') AS fill_instructions,
    NULLIF(product_name, '') AS product_name,
    NULLIF(revenue_sp, '')::NUMERIC AS revenue,
    NULLIF(pieces_produced_sp, '')::INTEGER AS pieces_produced,
    NULLIF(amount_per_piece_sp, '')::NUMERIC AS amount_per_piece
FROM {{ ref('staging_swep_production') }}
WHERE product_name IS NOT NULL