{{ config(
    materialized='table',
    tags=['upskilling', 'sl', 'beneficiary_tracking']
) }}

WITH extracted_data AS (
    SELECT
        id,

        -- Case details
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,

        -- Enumerator details (from meta_data)
        data::jsonb -> 'form' -> 'meta_data' ->> 'enumerator' AS fullname,
        data::jsonb -> 'form' -> 'meta_data' ->> 'enumerators_first' AS first_name,
        data::jsonb -> 'form' -> 'meta_data' ->> 'enumerator_last-name' AS last_name,

        -- Training details
        data::jsonb -> 'form' -> 'tailoring' ->> 'training_program_uf' AS training_program,
        data::jsonb -> 'form' -> 'tailoring' ->> 'year_trained_on_uf' AS year_trained,
        data::jsonb -> 'form' -> 'tailoring' ->> 'swep_center_uf' AS swep_center,

        -- Support details
        data::jsonb -> 'form' ->> 'additional_support_uf' AS additional_support,
        data::jsonb -> 'form' ->> 'educational_certificates_uf' AS educational_certificates,

        -- Dignity Kit details
        data::jsonb -> 'form' -> 'dignity_kit_support' ->> 'experienced_days_where_you_had_nothing_uf' AS experienced_days_where_you_had_nothing,
        data::jsonb -> 'form' -> 'dignity_kit_support' ->> 'miss_school_due_to_lack_hygiene_products' AS miss_school_due_to_lack_hygiene_products,
        data::jsonb -> 'form' -> 'dignity_kit_support' ->> 'enumerator_last-name' AS challenges_accessing_personal_hygiene_items,

        -- Child-related details
        data::jsonb -> 'form' -> 'daycare_support' ->> 'do_you_have_children_uf' AS has_children,
        data::jsonb -> 'form' -> 'daycare_support'->> 'children_under_4_uf' AS children_under_4,
        data::jsonb -> 'form' -> 'daycare_support'->> 'children_in_need_daycare_uf' AS children_in_daycare,
        data::jsonb -> 'form' -> 'daycare_support'->> 'children_withdisabilities_uf' AS children_with_disabilities,

        -- Upskilling interest
        data::jsonb -> 'form' ->> 'willing_to_relocate_uf' AS willing_to_relocate,
        data::jsonb -> 'form' ->> 'willing_to_be_upskilled_uf' AS willing_to_be_upskilled,
        data::jsonb -> 'form' ->> 'component_of_service_to_be_upskilled' AS upskilling_component,
        data::jsonb -> 'form' ->> 'do_you_recommend_beneficiary_to_proceed_uf' AS recommendation_status,

        -- Submission details
        data::jsonb ->> 'received_on' AS date_received

    FROM {{ source('staging_sl', 'Upskilling_Form') }}
    WHERE
        data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
)

SELECT DISTINCT
    id,
    case_id,
    user_id,
    fullname,
    first_name,
    last_name,
    training_program,
    swep_center,
    year_trained,
    additional_support,
    educational_certificates,
    has_children,
    children_under_4,
    children_in_daycare,
    children_with_disabilities,
    willing_to_relocate,
    willing_to_be_upskilled,
    upskilling_component,
    recommendation_status,
    date_received,
    experienced_days_where_you_had_nothing,
    miss_school_due_to_lack_hygiene_products,
    challenges_accessing_personal_hygiene_items
FROM extracted_data