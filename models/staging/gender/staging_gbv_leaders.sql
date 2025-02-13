{{ config(
  materialized='table',
  tags=["gender_gbv_leaders_and_champions", "gender"]
) }}
with gbv_cte as (
    select
        "National_ID" as national_id,
        "Active" as active,
        INITCAP(TRIM("County")) as county,
        INITCAP(TRIM("Gender")) as gender,
        "Mobile" as mobile,
        "Trained" as trained,
        "Identified" as identified,
        INITCAP(TRIM("Sub_county")) as sub_county,
        INITCAP(TRIM("Community_Role")) as community_role,
        "Date_identified" as date_identified,
        "Date_of_training" as date_trained,
        INITCAP(TRIM("GBV_Leader_Name")) as gbv_leader_name
    from {{ source('staging_gender', 'GBV_Community_Leaders') }}
)


{{ dbt_utils.deduplicate(
      relation='gbv_cte',
      partition_by='gbv_cte."national_id"',
      order_by='gbv_cte."date_identified"',
) }}
