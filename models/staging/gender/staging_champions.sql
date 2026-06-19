{{ config(
  materialized='table',
  tags=["gender_gbv_leaders_and_champions","gender"]
) }}

with champions_cte as (
    select
        INITCAP(TRIM("Champions_Name")) as champions_name,
        CASE 
            WHEN INITCAP(TRIM("County")) IN ('Transnzoia', 'Transznzoia') THEN 'Trans Nzoia'
            WHEN INITCAP(TRIM("County")) = 'Homabay' THEN 'Homa Bay'
            ELSE INITCAP(TRIM("County"))
        END as county,
        INITCAP(TRIM("Gender")) as gender, 
        "National_ID" as national_id,
        "Mobile" as mobile,
        INITCAP(TRIM("Sub_County_1")) as sub_county,
        "Site" as site,
        "Trained" as trained,
        "Identified" as identified,
        "Date_identified" as date_identified,
        "Date_trained" as date_trained,
        "Active" as active
    from {{ source('staging_gender', 'Champions_') }}
)

{{ dbt_utils.deduplicate(
      relation='champions_cte',
      partition_by='champions_cte."national_id"',
      order_by='champions_cte."date_identified"',
) }}
