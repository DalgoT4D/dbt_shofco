{{ config(
    materialized='table',
    tags=['swep', 'swep_sales', 'sl']
) }}

with source_data as (
    select
        id,
        data::jsonb as json_data
    from {{ source('staging_sl', 'swep_exhibition') }}
    WHERE
        data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
),

-- Base form info
base as (
    select
        id as form_id,
        json_data,

        json_data -> 'form' ->> 'recording_ses' as recording_ses,

        -- Common fields
        json_data -> 'form' -> 'meta' ->> 'username' as username,
        json_data -> 'form' ->> 'enumerator' as enumerator
    from source_data
),

-- Unnest only for sales forms
sales_exploded as (
    select
        form_id,
        recording_ses,
        username,
        enumerator,

        json_data -> 'form' -> 'sales_questions' ->> 'revenue_se'         as revenue_se,
        json_data -> 'form' -> 'sales_questions' ->> 'mpesa_code_se'      as mpesa_code_se,
        json_data -> 'form' -> 'sales_questions' ->> 'mpesa_name_se'      as mpesa_name_se,
        json_data -> 'form' -> 'sales_questions' ->> 'cost_of_product_se' as cost_of_product_se,
        json_data -> 'form' -> 'sales_questions' ->> 'units_sold_se'      as units_sold_se,
        json_data -> 'form' -> 'sales_questions' ->> 'product_sold_se'    as product_sold_se,
        json_data -> 'form' -> 'sales_questions' ->> 'fill_instructions'  as fill_instructions,

        -- exploded item-level data
        item -> 'details_of_sold_products' ->> 'revenue_se'         as item_revenue_se,
        item -> 'details_of_sold_products' ->> 'mpesa_code_se'      as item_mpesa_code_se,
        item -> 'details_of_sold_products' ->> 'mpesa_name_se'      as item_mpesa_name_se,
        item -> 'details_of_sold_products' ->> 'total_cost_se'      as item_total_cost_se,
        item -> 'details_of_sold_products' ->> 'units_sold_se'      as item_units_sold_se,
        item -> 'details_of_sold_products' ->> 'product_sold_se'    as item_product_sold_se,
        item -> 'details_of_sold_products' ->> 'fill_instructions'  as item_fill_instructions,
        item -> 'details_of_sold_products' ->> 'cost_of_product_se' as item_cost_of_product_se,

        -- NULLs for exhibition fields
        null as sales_made_se,
        null as date_of_exhibition_se,
        null as exhibition_attendees_se,
        null as location_of_exhibition_se,
        null as exhibition_event_attended_se

    from base,
    lateral jsonb_path_query(json_data, '$.form.sales_questions.sale_details[*]') as item
    where recording_ses = 'sales'
),

-- Read exhibition fields as-is
exhibition_flat as (
    select
        form_id,
        recording_ses,
        username,
        enumerator,

        -- NULLs for sales-specific fields
        null as revenue_se,
        null as mpesa_code_se,
        null as mpesa_name_se,
        null as cost_of_product_se,
        null as units_sold_se,
        null as product_sold_se,
        null as fill_instructions,

        -- NULLs for sale item breakdowns
        null as item_revenue_se,
        null as item_mpesa_code_se,
        null as item_mpesa_name_se,
        null as item_total_cost_se,
        null as item_units_sold_se,
        null as item_product_sold_se,
        null as item_fill_instructions,
        null as item_cost_of_product_se,

        -- exhibition fields
        json_data -> 'form' -> 'exhibition_questions' ->> 'sales_made_se'                as sales_made_se,
        json_data -> 'form' -> 'exhibition_questions' ->> 'date_of_exhibition_se'        as date_of_exhibition_se,
        json_data -> 'form' -> 'exhibition_questions' ->> 'exhibition_attendees_se'      as exhibition_attendees_se,
        json_data -> 'form' -> 'exhibition_questions' ->> 'location_of_exhibition_se'    as location_of_exhibition_se,
        json_data -> 'form' -> 'exhibition_questions' ->> 'exhibition_event_attended_se' as exhibition_event_attended_se

    from base
    where recording_ses = 'exhibition'
)

-- Combine both
select * from sales_exploded
union all
select * from exhibition_flat