{{ config(
  materialized='table',
  tags=["gender_gbv_leaders_and_champions", "gender"]
) }}
with gbv_cte as (
    select
        "National_ID",
        "Active",
        INITCAP(TRIM("County")) as "County",
        INITCAP(TRIM("Gender")) as "Gender",
        "Mobile",
        "Trained",
        "Identified",
        INITCAP(TRIM("Sub_county")) as "Sub_county",
        INITCAP(TRIM("Community_Role")) as "Community_Role",
        TO_DATE("Date_identified", 'DD/MM/YYYY') as "Date_identified",
        TO_DATE("Date_of_training", 'DD/MM/YYYY') as "Date_trained",
        INITCAP(TRIM("GBV_Leader_Name")) as "GBV_Leader_Name"
    from {{ source('staging_gender', 'GBV_Community_Leaders') }}
)


{{ dbt_utils.deduplicate(
      relation='gbv_cte',
      partition_by='gbv_cte."National_ID"',
      order_by='gbv_cte."Date_identified"',
) }}
