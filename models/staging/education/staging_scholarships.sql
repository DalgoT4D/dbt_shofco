{{ config(
  materialized='table',
  tags="education_expansion"
) }}

SELECT
    *
FROM {{ source('staging_education', 'Scholarships') }}