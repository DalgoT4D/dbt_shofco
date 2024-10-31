{{ config(
  materialized='table'
) }}

SELECT
    *
FROM {{ source('staging_education', 'Public_School_Partnerships') }}