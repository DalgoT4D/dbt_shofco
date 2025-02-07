{{ config(
  materialized='table',
  tags=['org_mapping', "org_wide"]
) }}

SELECT
    id,
    case_id,
    case_type,
    facility,
    school_name,
    type_of_school,
    ward,
    county,
    constituency,
    office_gender_desk_location,
    CAST(received_on AS DATE) AS received_on,

    -- Extract GPS components from gps_location field
    CAST (SPLIT_PART(gps_location, ' ', 1) AS FLOAT) AS latitude,
    CAST (SPLIT_PART(gps_location, ' ', 2) AS FLOAT) AS longitude,
    CAST (SPLIT_PART(gps_location, ' ', 3) AS FLOAT) AS altitude,
    CAST (SPLIT_PART(gps_location, ' ', 4) AS FLOAT) AS accuracy,

    -- Create "Active Programs" column based on program columns
    CASE
        WHEN
            wash_program = 'yes'
            AND gender_program = 'yes'
            AND shofco_education_program = 'yes'
            THEN 'WASH, Gender, Education'
        WHEN
            wash_program = 'yes' AND gender_program = 'yes'
            THEN 'WASH, Gender'
        WHEN
            wash_program = 'yes' AND shofco_education_program = 'yes'
            THEN 'WASH, Education'
        WHEN
            gender_program = 'yes' AND shofco_education_program = 'yes'
            THEN 'Gender, Education'
        WHEN wash_program = 'yes' THEN 'WASH'
        WHEN gender_program = 'yes' THEN 'Gender'
        WHEN shofco_education_program = 'yes' THEN 'Education'
    END AS active_programs,

    -- Concatenate intervention into a single column
    CASE
        WHEN
            gender_intervention IS NOT NULL
            AND wash_intervention IS NOT NULL
            AND education_intervention IS NOT NULL
            THEN
                gender_intervention
                || ', '
                || wash_intervention
                || ', '
                || education_intervention
        WHEN
            gender_intervention IS NOT NULL AND wash_intervention IS NOT NULL
            THEN
                gender_intervention || ', ' || wash_intervention
        WHEN
            gender_intervention IS NOT NULL
            AND education_intervention IS NOT NULL
            THEN
                gender_intervention || ', ' || education_intervention
        WHEN
            wash_intervention IS NOT NULL AND education_intervention IS NOT NULL
            THEN
                wash_intervention || ', ' || education_intervention
        WHEN gender_intervention IS NOT NULL THEN gender_intervention
        WHEN wash_intervention IS NOT NULL THEN wash_intervention
        WHEN education_intervention IS NOT NULL THEN education_intervention
    END AS intervention


FROM {{ ref('staging_mapping') }}
