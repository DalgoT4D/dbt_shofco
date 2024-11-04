{{ config(
  materialized='table'
) }}

SELECT
    "Active",
    INITCAP(TRIM("County")) AS "County",
    INITCAP(TRIM("Gender")) AS "Gender",
    "Mobile",
    "Trained",
    "Identified",
    INITCAP(TRIM("Sub_county")) AS "Sub_county",
    "National_ID",
    INITCAP(TRIM("Community_Role")) AS "Community_Role",
    CAST("Date_identified" AS DATE) AS "Date_identified",
    CAST("Date_of_training" AS DATE) AS "Date_trained",
    INITCAP(TRIM("GBV_Leader_Name")) AS "GBV_Leader_Name"
FROM {{ source('staging_gender', 'GBV_Community_Leaders') }}
