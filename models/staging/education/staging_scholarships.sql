{{ config(
  materialized='table'
) }}

SELECT
    *
FROM {{ source('staging_education', 'Scholarships') }}