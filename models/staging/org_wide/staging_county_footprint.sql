{{ config(
  materialized='table',
  tags="org_mapping"
) }}

SELECT
    *
FROM {{ source('staging_orgwide', 'County_Footprint') }}