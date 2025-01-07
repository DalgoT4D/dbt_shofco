{{ config(
  materialized='table',
  tags="org_mapping"
) }}

SELECT
  "SL",
  "SUN",
  "WASH",
  "SACCO",
  "County",
  "Gender",
  "Health",
  "latitude",
  "Education",
  "Libraries",
  "longitude",
  "Youth_Voice",
  "iso_3166_2_code",
  "_airbyte_raw_id",
  "_airbyte_extracted_at",
  "_airbyte_meta"
FROM {{ source('staging_orgwide', 'County_Footprint') }}