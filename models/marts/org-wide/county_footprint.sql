{{ config(
  materialized='table'
) }}

SELECT
    "County",
    "iso_3166_2_code",
    "latitude",
    "longitude",
    -- Transform each program column to numeric
    CASE WHEN "SUN" = 'Y' THEN 1 ELSE 0 END AS SUN,
    CASE WHEN "Youth_Voice" = 'Y' THEN 1 ELSE 0 END AS Youth_Voice,
    CASE WHEN "SACCO" = 'Y' THEN 1 ELSE 0 END AS SACCO,
    CASE WHEN "SL" = 'Y' THEN 1 ELSE 0 END AS SL,
    CASE WHEN "Education" = 'Y' THEN 1 ELSE 0 END AS Education,
    CASE WHEN "Gender" = 'Y' THEN 1 ELSE 0 END AS Gender,
    CASE WHEN "WASH" = 'Y' THEN 1 ELSE 0 END AS WASH,
    CASE WHEN "Health" = 'Y' THEN 1 ELSE 0 END AS Health,
    CASE WHEN "Libraries" = 'Y' THEN 1 ELSE 0 END AS Libraries
FROM {{ ref("staging_county_footprint") }}