{{ config(
  materialized = 'table',
  tags = ['swep', 'swep_exhibition', 'swep_production', 'sl']
) }}

WITH source_data AS (
    SELECT
        id,
        data
    FROM {{ source('staging_sl', 'SWEP_Exhibition_and_Sales') }}
    WHERE data::jsonb -> 'form' ->> '@name' = 'SWEP Exhibition and Sales'
),

base AS (
    SELECT
        id AS form_id,
        data::jsonb AS json_data
    FROM source_data
),

sales_records AS (
    SELECT
        json_data -> 'form' -> 'case' -> 'create' ->> 'case_name' AS case_name,
        json_data -> 'form' -> 'case' ->> '@case_id' AS case_id,
        json_data -> 'form' -> 'case' ->> '@user_id' AS user_id,
        json_data -> 'form' -> 'case' -> 'create' ->> 'owner_id' AS owner_id,
        json_data -> 'form' -> 'case' -> 'update' ->> 'recording_ses' AS recording_ses,
        json_data -> 'form' -> 'case' -> 'update' ->> 'enumerator' AS enumerator_full_name,
        json_data -> 'form' -> 'case' -> 'update' ->> 'enumerators_first' AS enumerator_first,
        json_data -> 'form' -> 'case' -> 'update' ->> 'enumerator_last-name' AS enumerator_last,

        json_data -> 'form' -> 'sales_questions' ->> 'date_of_sale_ses' AS date_of_sale,
        json_data -> 'form' -> 'sales_questions' ->> 'unique_items_sold' AS unique_items_sold,
        json_data -> 'form' -> 'exhibition_questions' ->> 'date_of_exhibition_se' AS date_of_exhibition,
        json_data -> 'form' -> 'exhibition_questions' ->> 'location_of_exhibition_se' AS location_of_exhibition,
        json_data -> 'form' -> 'exhibition_questions' ->> 'exhibition_event_attended_se' AS exhibition_event_attended,
        json_data -> 'form' -> 'exhibition_questions' ->> 'exhibition_attendees_se' AS exhibition_attendees,
        json_data -> 'form' -> 'exhibition_questions' ->> 'sales_made_se' AS sales_made,
        jsonb_path_query(json_data, '$.form.sales_questions.sale_details[*]') AS sale_item
    FROM base
    WHERE LOWER(json_data -> 'form' -> 'case' -> 'create' ->> 'case_name') = 'sales'
),

exploded_sales AS (
    SELECT
        case_name,
        case_id,
        user_id,
        owner_id,
        recording_ses,
        enumerator_full_name,
        enumerator_first,
        enumerator_last,
        date_of_exhibition,
        location_of_exhibition,
        exhibition_event_attended,
        exhibition_attendees,
        sales_made,
        date_of_sale,
        unique_items_sold,
        sale_item::jsonb -> 'details_of_sold_products' ->> 'product_sold_se' AS product_name,
        sale_item::jsonb -> 'details_of_sold_products' ->> 'units_sold_se' AS units_sold,
        sale_item::jsonb -> 'details_of_sold_products' ->> 'cost_of_product_se' AS cost_of_product_sp,
        sale_item::jsonb -> 'details_of_sold_products' ->> 'revenue_se' AS revenue_sp,
        sale_item::jsonb -> 'details_of_sold_products' ->> 'total_cost_se' AS total_cost_sp,
        sale_item::jsonb -> 'details_of_sold_products' ->> 'fill_instructions' AS fill_instructions_sp,
        sale_item::jsonb -> 'details_of_sold_products' ->> 'mpesa_code_se' AS mpesa_code_sp,
        sale_item::jsonb -> 'details_of_sold_products' ->> 'mpesa_name_se' AS mpesa_name_sp
    FROM sales_records
),

exhibition_only AS (
    SELECT
        json_data -> 'form' -> 'case' -> 'create' ->> 'case_name' AS case_name,
        json_data -> 'form' -> 'case' ->> '@case_id' AS case_id,
        json_data -> 'form' -> 'case' ->> '@user_id' AS user_id,
        json_data -> 'form' -> 'case' -> 'create' ->> 'owner_id' AS owner_id,
        json_data -> 'form' -> 'case' -> 'update' ->> 'recording_ses' AS recording_ses,
        json_data -> 'form' -> 'case' -> 'update' ->> 'enumerator' AS enumerator_full_name,
        json_data -> 'form' -> 'case' -> 'update' ->> 'enumerators_first' AS enumerator_first,
        json_data -> 'form' -> 'case' -> 'update' ->> 'enumerator_last-name' AS enumerator_last,
        COALESCE(json_data -> 'form' -> 'exhibition_questions' ->> 'date_of_exhibition_se',
                 json_data -> 'form' -> 'case' -> 'update' ->> 'date_of_exhibition_se') AS date_of_exhibition,
        COALESCE(json_data -> 'form' -> 'exhibition_questions' ->> 'location_of_exhibition_se',
                 json_data -> 'form' -> 'case' -> 'update' ->> 'location_of_exhibition_se') AS location_of_exhibition,
        COALESCE(json_data -> 'form' -> 'exhibition_questions' ->> 'exhibition_event_attended_se',
                 json_data -> 'form' -> 'case' -> 'update' ->> 'exhibition_event_attended_se') AS exhibition_event_attended,
        COALESCE(json_data -> 'form' -> 'exhibition_questions' ->> 'exhibition_attendees_se',
                 json_data -> 'form' -> 'case' -> 'update' ->> 'exhibition_attendees_se') AS exhibition_attendees,
        COALESCE(json_data -> 'form' -> 'exhibition_questions' ->> 'sales_made_se',
                 json_data -> 'form' -> 'case' -> 'update' ->> 'sales_made_se') AS sales_made,
        NULL AS date_of_sale,
        NULL AS unique_items_sold,
        NULL AS product_name,
        NULL AS units_sold,
        NULL AS cost_of_product_sp,
        NULL AS revenue_sp,
        NULL AS total_cost_sp,
        NULL AS fill_instructions_sp,
        NULL AS mpesa_code_sp,
        NULL AS mpesa_name_sp
    FROM base
    WHERE LOWER(json_data -> 'form' -> 'case' -> 'create' ->> 'case_name') = 'exhibition'
)

SELECT * FROM exploded_sales
UNION ALL
SELECT * FROM exhibition_only
