{{ config(
  materialized='table'
) }}

SELECT
    "Year" as "year",
    "School_Name" as "school_name",
    "School_County" as "county",
    "Mean_KPCE_Score",
    "School_Subcounty" as "subcounty",
    "__Teachers_trained",
    "__Students_enrolled",
    "__Computers_provided",
    "__Toilet_stances_built",
    "High_Touch___Low_Touch"
FROM {{ ref("staging_public_partnerships") }}