{{ config(
  materialized='table',
  tags='org_mapping'
) }}

SELECT
    *,
    
    -- Extract GPS components from gps_location field
    split_part("gps_location", ' ', 1)::float AS "latitude",
    split_part("gps_location", ' ', 2)::float AS "longitude",
    split_part("gps_location", ' ', 3)::float AS "altitude",
    split_part("gps_location", ' ', 4)::float AS "accuracy",

    -- Create "Active Programs" column based on program columns
    CASE
        WHEN "wash_program" = 'yes' AND "gender_program" = 'yes' AND "shofco_education_program" = 'yes' THEN 'WASH, Gender, Education'
        WHEN "wash_program" = 'yes' AND "gender_program" = 'yes' THEN 'WASH, Gender'
        WHEN "wash_program" = 'yes' AND "shofco_education_program" = 'yes' THEN 'WASH, Education'
        WHEN "gender_program" = 'yes' AND "shofco_education_program" = 'yes' THEN 'Gender, Education'
        WHEN "wash_program" = 'yes' THEN 'WASH'
        WHEN "gender_program" = 'yes' THEN 'Gender'
        WHEN "shofco_education_program" = 'yes' THEN 'Education'
        ELSE NULL
    END AS "Active Programs",
    
    -- Concatenate intervention into a single column
    CASE
        WHEN "gender_intervention" IS NOT NULL AND "wash_intervention" IS NOT NULL AND "education_intervention" IS NOT NULL THEN 
            "gender_intervention" || ', ' || "wash_intervention" || ', ' || "education_intervention"
        WHEN "gender_intervention" IS NOT NULL AND "wash_intervention" IS NOT NULL THEN 
            "gender_intervention" || ', ' || "wash_intervention"
        WHEN "gender_intervention" IS NOT NULL AND "education_intervention" IS NOT NULL THEN 
            "gender_intervention" || ', ' || "education_intervention"
        WHEN "wash_intervention" IS NOT NULL AND "education_intervention" IS NOT NULL THEN 
            "wash_intervention" || ', ' || "education_intervention"
        WHEN "gender_intervention" IS NOT NULL THEN "gender_intervention"
        WHEN "wash_intervention" IS NOT NULL THEN "wash_intervention"
        WHEN "education_intervention" IS NOT NULL THEN "education_intervention"
        ELSE NULL
    END AS "intervention"


FROM {{ ref('staging_mapping') }}