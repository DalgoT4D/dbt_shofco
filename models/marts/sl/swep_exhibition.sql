{{
  config(
    materialized='table',
    tags=['swep', 'swep_exhibition_sales', 'sl', "sl_marts"]
  )
}}

SELECT
  id AS form_id,
  recording_ses,
  user_id AS username,
  enumerator,

  -- sales-level fields
  NULLIF(revenue_sp, '')::NUMERIC AS revenue,
  NULL::NUMERIC AS cost_of_product,
  NULL::INTEGER AS units_sold,
  product_made_sp AS product_sold,
  NULL AS fill_instructions,

  -- item-level sales breakdown
  NULLIF(revenue_sp, '')::NUMERIC AS item_revenue,
  NULL AS item_mpesa_code,
  MPESA_full_name_sp AS item_mpesa_name,
  NULL AS item_total_cost,
  pieces_produced_sp::INTEGER AS item_units_sold,
  product_made_sp AS item_product_sold,
  NULL AS item_fill_instructions,
  NULL::NUMERIC AS item_cost_of_product,

  -- exhibition-only fields
  NULL AS sales_made,
  date_of_production_sp AS date_of_exhibition,
  NULL AS exhibition_attendees,
  NULL AS exhibition_location,
  NULL AS exhibition_event_attended

FROM {{ ref('staging_sl_case_table') }}
WHERE recording_ses IN ('sales', 'exhibition')