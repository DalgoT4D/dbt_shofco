{{ config(
  materialized='table',
  tags=["gender_gbv_leaders_and_champions","gender"]
) }}

with champions_cte as (
    select
        INITCAP(TRIM("Champions_Name")) as "Champions_Name",
        INITCAP(TRIM("Community_Role")) as "Community_Role",
        INITCAP(TRIM("County")) as "County",
        INITCAP(TRIM("Gender")) as "Gender", 
        "National_ID",
        "Mobile",
        INITCAP(TRIM("Sub_County_1")) as "Sub_County",
        "Site",
        "Trained",
        "Identified",
        {{ validate_date("Date_identified") }} AS "Date_identified",
        {{ validate_date("Date_trained") }} as "Date_trained",
        "Active"
    from {{ source('staging_gender', 'Champions_') }}
)

{{ dbt_utils.deduplicate(
      relation='champions_cte',
      partition_by='champions_cte."National_ID"',
      order_by='champions_cte."Date_identified"',
) }}
