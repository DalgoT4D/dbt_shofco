{{
  config(
    materialized='table',
    tags=['swep', 'swep_exhibition_sales', 'sl']
  )
}}

SELECT
  form_id,
  recording_ses,
  username,
  enumerator,

  -- sales-level fields
  NULLIF(revenue_se, '')::NUMERIC AS revenue,
  NULLIF(mpesa_code_se, '') AS mpesa_code,
  NULLIF(mpesa_name_se, '') AS mpesa_name,
  NULLIF(cost_of_product_se, '')::NUMERIC AS cost_of_product,
  NULLIF(units_sold_se, '')::INTEGER AS units_sold,
  NULLIF(product_sold_se, '') AS product_sold,
  fill_instructions,

  -- item-level sales breakdown
  NULLIF(item_revenue_se, '')::NUMERIC AS item_revenue,
  NULLIF(item_mpesa_code_se, '') AS item_mpesa_code,
  NULLIF(item_mpesa_name_se, '') AS item_mpesa_name,
  NULLIF(item_total_cost_se, '') AS item_total_cost,
  NULLIF(item_units_sold_se, '')::INTEGER AS item_units_sold,
  NULLIF(item_product_sold_se, '') AS item_product_sold,
  item_fill_instructions,
  NULLIF(item_cost_of_product_se, '')::NUMERIC AS item_cost_of_product,

  -- exhibition-only fields
  NULLIF(sales_made_se, '') AS sales_made,
  {{ validate_date('date_of_exhibition_se') }} AS date_of_exhibition,
  NULLIF(exhibition_attendees_se, '') AS exhibition_attendees,
  NULLIF(location_of_exhibition_se, '') AS exhibition_location,
  NULLIF(exhibition_event_attended_se, '') AS exhibition_event_attended

FROM {{ ref('staging_swep_exhibition_sales') }}
WHERE recording_ses IN ('sales', 'exhibition')