{{ config(
  materialized='table',
  tags=["education_expansion", "education"]
) }}

SELECT
    "Term" AS term,
    "Year" AS year,
    "Cohort" AS cohort,
    "CURRENT_GRADE" AS grade,
    "NAME_OF_PUPIL" AS name,
    "PRIMARY_SCHOOL" AS primary_school,
    "NUDGE_TYPE_RECEIVED" AS nudge_type,
    LOWER("COUNTY") AS county,
    LOWER("GENDER") AS gender,
    LOWER("SUBCOUNTY") AS subcounty
FROM {{ source('staging_education', 'Nudges') }}
