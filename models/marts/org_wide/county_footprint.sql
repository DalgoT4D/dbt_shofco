{{ config(
  materialized='table',
  tags='org_mapping'
) }}

SELECT
    "County" AS county,
    iso_3166_2_code,
    latitude,
    longitude,
    -- Transform each program column to numeric
    CASE WHEN "SUN" = 'Y' THEN 1 ELSE 0 END AS sun,
    CASE WHEN "Youth_Voice" = 'Y' THEN 1 ELSE 0 END AS youth_voice,
    CASE WHEN "SACCO" = 'Y' THEN 1 ELSE 0 END AS sacco,
    CASE WHEN "SL" = 'Y' THEN 1 ELSE 0 END AS sl,
    CASE WHEN "Education" = 'Y' THEN 1 ELSE 0 END AS education,
    CASE WHEN "Gender" = 'Y' THEN 1 ELSE 0 END AS gender,
    CASE WHEN "WASH" = 'Y' THEN 1 ELSE 0 END AS wash,
    CASE WHEN "Health" = 'Y' THEN 1 ELSE 0 END AS health,
    CASE WHEN "Libraries" = 'Y' THEN 1 ELSE 0 END AS libraries
FROM {{ ref("staging_county_footprint") }}

