{{ config(
  materialized='table'
) }}

SELECT
    INITCAP(TRIM("Champions_Name")) AS "Champions_Name",
    INITCAP(TRIM("Community_Role")) AS "Community_Role",
    INITCAP(TRIM("County")) AS "County",
    INITCAP(TRIM("Gender")) AS "Gender", 
    "National_ID",
    "Mobile",
    INITCAP(TRIM("Sub_County_1")) AS "Sub_County",
    "Site",
    "Trained",
    "Identified",
    CAST("Date_identified" AS DATE) AS "Date_identified",
    CAST("Date_trained" AS DATE) AS "Date_trained",
    "Active"
FROM {{ source('staging_gender', 'Champions_') }}
