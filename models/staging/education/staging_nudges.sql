{{ config(
  materialized='table',
  tags="education_expansion"
) }}

SELECT
    "Term" as "term",
    "Year" as "year",
    LOWER("COUNTY") as "county",
    "Cohort" as "cohort",	
    LOWER("GENDER") AS "gender",
    LOWER("SUBCOUNTY") AS "subcounty",	
    "CURRENT_GRADE" as "grade",	
    "NAME_OF_PUPIL" as "name",
    "PRIMARY_SCHOOL" as "primary_school",
    "NUDGE_TYPE_RECEIVED" as "nudge_type"
FROM {{ source('staging_education', 'Nudges') }}