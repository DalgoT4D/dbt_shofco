{{ config(
  materialized='table',
  tags="education_expansion"
) }}

SELECT
    "Term",
    "Year",
    "COUNTY",
    "Cohort",	
    "GENDER",
    "SUBCOUNTY",	
    "CURRENT_GRADE",	
    "NAME_OF_PUPIL",
    "PRIMARY_SCHOOL",
    "NUDGE_TYPE_RECEIVED",
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta",
FROM {{ source('staging_education', 'Nudges') }}