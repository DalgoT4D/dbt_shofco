{{ config(
  materialized='table'
) }}

SELECT
    INITCAP(TRIM("GBV_Leader_Name")) AS "Name",
    INITCAP(TRIM("County")) AS "County",
    CASE
        WHEN TRIM(LOWER("Gender")) = 'm' THEN 'Male'
        ELSE INITCAP(TRIM("Gender"))
    END AS "Gender",
    "Mobile",
    INITCAP(TRIM("Sub_county")) AS "Sub_county",
    "National_ID",
    INITCAP(TRIM("Community_Role")) AS "Community_Role"

FROM {{ ref('staging_gbv_leaders') }}  -- Refers to the previous model