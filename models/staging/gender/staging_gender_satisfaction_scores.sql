{{ config(
  materialized='table'
) }}

SELECT
        (data::json->'form'->>'date_of_visit') AS date_of_visit,
        -- Extract the overall satisfaction score from the new JSON structure
        (data::json->'form'->'genaral_gender_services'->'overral_satisfaction'->>'on_a_scale_of_1-5_where_1_is_very_dissatisfied_and_5_is_very_satisfied_over')::INTEGER AS overall_satisfaction
    FROM {{ source('staging_gender', 'Gender_Clients_Feedback_Survey') }}
    WHERE data::json->'form'->>'date_of_visit' IS NOT NULL