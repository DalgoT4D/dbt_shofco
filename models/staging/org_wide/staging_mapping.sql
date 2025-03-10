{{ config(
  materialized='table',
  tags=["org_mapping", "org_wide"]
) }}

WITH combined_case_data AS (
    SELECT
        id,
        data::jsonb->'form'->'case'->'@case_id' AS case_id,
        data::jsonb->'form'->'case'->'create'->>'case_type' AS case_type,  -- Extract the case type
        data::jsonb->'form'->>'comments' AS comments,
        data::jsonb->'form'->>'facility' AS facility,
        data::jsonb->'form'->'school_information'->>'school_name' AS school_name,
        data::jsonb->'form'->'school_information'->>'wash_program' AS wash_program,
        data::jsonb->'form'->'school_information'->>'gender_program' AS gender_program,
        NULLIF(data::jsonb->'form'->'school_information'->>'gender_intervention', '') AS gender_intervention,
        NULLIF(data::jsonb->'form'->'school_information'->>'education_intervention', '') AS education_intervention,
        NULLIF(data::jsonb->'form'->'school_information'->>'which_wash_interventions_are_in_the_school', '') AS wash_intervention,
        data::jsonb->'form'->'school_information'->>'type_of_school' AS type_of_school,
        data::jsonb->'form'->'school_information'->>'does_the_school_have_shofco_education_program' AS shofco_education_program,
        data::jsonb->'form'->'geographical_location'->>'ward' AS ward,
        data::jsonb->'form'->'geographical_location'->>'county' AS county,
        data::jsonb->'form'->'geographical_location'->>'constituency' AS constituency,
        data::jsonb->'form'->'geographical_location'->>'name_of_the_interviewer' AS interviewer_name,
        data::jsonb->'form'->>'gps_location_of_the_facility' AS gps_location,
        data::jsonb->'form'->>'officegender_desk_locationname' AS office_gender_desk_location,
        data::jsonb->>'received_on' AS received_on,
        data::jsonb->>'type' AS data_type
    FROM {{ source('staging_gender', 'IIVC_Mapping') }}
    WHERE
        data::jsonb ->> 'archived' IS NULL
        OR data::jsonb ->> 'archived' = 'false'
)

SELECT DISTINCT
    id,
    case_id,
    case_type,
    comments,
    facility,
    gender_intervention,
    wash_intervention,
    education_intervention,
    school_name,
    wash_program,
    gender_program,
    type_of_school,
    shofco_education_program,
    ward,
    county,
    constituency,
    interviewer_name,
    gps_location,
    office_gender_desk_location,
    {{ validate_date("received_on") }} AS received_on
FROM combined_case_data
