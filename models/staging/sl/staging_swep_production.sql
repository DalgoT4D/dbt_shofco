{{ config(
  materialized = 'table',
  tags = ['swep', 'swep_production', 'sl']
) }}

WITH source_data AS (
    SELECT
        id,
        data
    FROM {{ source('staging_sl', 'SWEP_Production_Details') }}
    WHERE data::jsonb -> 'form' ->> '@name' = 'SWEP Exhibition and Sales'
),

base AS (
    SELECT
        id AS form_id,
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' -> 'create' ->> 'owner_id' AS owner_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'recording_ses' AS recording_ses,

        -- Sales metadata
        data::jsonb -> 'form' -> 'sales_questions' ->> 'date_of_sale_ses' AS date_of_sale,
        data::jsonb -> 'form' -> 'sales_questions' ->> 'unique_items_sold' AS unique_items_sold,

        -- Exhibition metadata
        COALESCE(
            data::jsonb -> 'form' -> 'exhibition_questions' ->> 'date_of_exhibition_se',
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'date_of_exhibition_se'
        ) AS date_of_exhibition,

        COALESCE(
            data::jsonb -> 'form' -> 'exhibition_questions' ->> 'location_of_exhibition_se',
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'location_of_exhibition_se'
        ) AS location_of_exhibition,

        COALESCE(
            data::jsonb -> 'form' -> 'exhibition_questions' ->> 'exhibition_event_attended_se',
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'exhibition_event_attended_se'
        ) AS exhibition_event_attended,

        COALESCE(
            data::jsonb -> 'form' -> 'exhibition_questions' ->> 'exhibition_attendees_se',
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'exhibition_attendees_se'
        ) AS exhibition_attendees,

        COALESCE(
            data::jsonb -> 'form' -> 'exhibition_questions' ->> 'sales_made_se',
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'sales_made_se'
        ) AS sales_made,

        -- Explode each sold item
        jsonb_path_query(data::jsonb, '$.form.sales_questions.sale_details[*]') AS product_item
    FROM source_data
),

exploded AS (
    SELECT
        form_id,
        case_id,
        owner_id,
        user_id,
        recording_ses,
        date_of_sale,
        unique_items_sold,
        date_of_exhibition,
        location_of_exhibition,
        exhibition_event_attended,
        exhibition_attendees,
        sales_made,

        product_item::jsonb -> 'details_of_sold_products' ->> 'product_sold_se' AS product_name,
        product_item::jsonb -> 'details_of_sold_products' ->> 'units_sold_se' AS units_sold,
        product_item::jsonb -> 'details_of_sold_products' ->> 'revenue_se' AS revenue_sp,
        product_item::jsonb -> 'details_of_sold_products' ->> 'cost_of_product_se' AS cost_of_product_sp,
        product_item::jsonb -> 'details_of_sold_products' ->> 'total_cost_se' AS total_cost_sp,
        product_item::jsonb -> 'details_of_sold_products' ->> 'fill_instructions' AS fill_instructions_sp,
        product_item::jsonb -> 'details_of_sold_products' ->> 'mpesa_code_se' AS mpesa_code_sp,
        product_item::jsonb -> 'details_of_sold_products' ->> 'mpesa_name_se' AS mpesa_name_sp
    FROM base
)

SELECT *
FROM exploded
