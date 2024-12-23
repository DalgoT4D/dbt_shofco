{{ config(
  materialized='table',
  tags="education_expansion"
) }}

SELECT
    *
FROM {{ source('staging_education', 'Public_School_Partnerships') }}