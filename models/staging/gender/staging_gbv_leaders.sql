{{ config(
  materialized='table',
  tags="gender_gbv_leaders_and_champions"
) }}
with gbv_cte as (
  SELECT
      "National_ID",
      "Active",
      INITCAP(TRIM("County")) AS "County",
      INITCAP(TRIM("Gender")) AS "Gender",
      "Mobile",
      "Trained",
      "Identified",
      INITCAP(TRIM("Sub_county")) AS "Sub_county",
      INITCAP(TRIM("Community_Role")) AS "Community_Role",
      TO_DATE("Date_identified", 'DD/MM/YYYY') AS "Date_identified",
      TO_DATE("Date_of_training", 'DD/MM/YYYY') AS "Date_trained",
      INITCAP(TRIM("GBV_Leader_Name")) AS "GBV_Leader_Name"
  FROM {{ source('staging_gender', 'GBV_Community_Leaders') }}
)


{{ dbt_utils.deduplicate(
      relation='gbv_cte',
      partition_by='gbv_cte."National_ID"',
      order_by='gbv_cte."Date_identified"',
) }}