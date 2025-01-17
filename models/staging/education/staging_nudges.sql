{{ config(
  materialized='table',
  tags="education_expansion"
) }}

SELECT
    "Term" AS term,
    "Year" AS year,
    LOWER("COUNTY") AS county,
    "Cohort" AS cohort,	
    LOWER("GENDER") AS gender,
    LOWER("SUBCOUNTY") AS subcounty,	
    "CURRENT_GRADE" AS grade,	
    "NAME_OF_PUPIL" AS name,
    "PRIMARY_SCHOOL" AS primary_school,
    "NUDGE_TYPE_RECEIVED" AS nudge_type
FROM {{ source('staging_education', 'Nudges') }}
