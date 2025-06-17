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
        -- Enumerator details  
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator' AS fullname,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerators_first' AS first_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator_last-name' AS last_name,
        -- Training details  
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'training_program_uf' AS training_program,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'swep_center_uf' AS swep_center,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'year_trained_on_uf' AS year_trained,
        -- Support details  
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'additional_support_uf' AS additional_support,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'educational_certificates_uf' AS educational_certificates,
        -- Child-related details  
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'do_you_have_children_uf' AS has_children,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'children_under_4_uf' AS children_under_4,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'children_in_need_daycare_uf' AS children_in_daycare,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'children_withdisabilities_uf' AS children_with_disabilities,
        -- Upskilling interest  
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'willing_to_relocate_uf' AS willing_to_relocate,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'willing_to_be_upskilled_uf' AS willing_to_be_upskilled,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'component_of_service_to_be_upskilled' AS upskilling_component,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'do_you_recommend_beneficiary_to_proceed_uf' AS recommendation_status,
        -- Submission details  
        data::jsonb ->> 'received_on' AS date_received
    FROM {{ source('staging_sl', 'Upskilling_Form') }}
    WHERE COALESCE(data::jsonb ->> 'archived', 'false') = 'false'  -- Using COALESCE to handle NULL values
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
    date_received
FROM extracted_data
