{{ config(
  materialized='table'
) }}

SELECT
    *
FROM {{ source('staging_orgwide', 'County_Footprint') }}