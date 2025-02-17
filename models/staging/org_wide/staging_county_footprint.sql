{{ config(
  materialized='table',
  tags=["org_mapping", "org_wide"]
) }}

SELECT
    COALESCE("SL", 'N') AS sl,
    COALESCE("SUN", 'N') AS sun,
    COALESCE("WASH", 'N') AS wash,
    COALESCE("SACCO", 'N') AS sacco,
    COALESCE("County", 'N') AS county,
    COALESCE("Gender", 'N') AS gender,
    COALESCE("Health", 'N') AS health,
    latitude,
    COALESCE("Education", 'N') AS education,
    COALESCE("Libraries", 'N') AS libraries,
    longitude,
    COALESCE("Youth_Voice", 'N') AS youth_voice,
    iso_3166_2_code,
    "_airbyte_extracted_at" AS _airbyte_extracted_at
FROM {{ source('staging_orgwide', 'County_Footprint') }}
