{{ config(
  materialized='table',
  tags=['org_mapping', "org_wide"]
) }}

SELECT
    county,
    iso_3166_2_code,
    latitude,
    longitude,
    -- Transform each program column to numeric
    CASE WHEN sun = 'Y' THEN 1 ELSE 0 END AS sun,
    CASE WHEN youth_voice = 'Y' THEN 1 ELSE 0 END AS youth_voice,
    CASE WHEN sacco = 'Y' THEN 1 ELSE 0 END AS sacco,
    CASE WHEN sl = 'Y' THEN 1 ELSE 0 END AS sl,
    CASE WHEN education = 'Y' THEN 1 ELSE 0 END AS education,
    CASE WHEN gender = 'Y' THEN 1 ELSE 0 END AS gender,
    CASE WHEN wash = 'Y' THEN 1 ELSE 0 END AS wash,
    CASE WHEN health = 'Y' THEN 1 ELSE 0 END AS health,
    CASE WHEN libraries = 'Y' THEN 1 ELSE 0 END AS libraries
FROM {{ ref("staging_county_footprint") }}