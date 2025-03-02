{{ config(
  materialized='table',
  tags=['wash_facilities',"wash"]
) }}

select
"ward",
"county",
"village",
REGEXP_REPLACE("latitude", '[^0-9.-]', '') AS "latitude", 
REGEXP_REPLACE("longitude", '[^0-9.-]', '') AS "longitude", 
"subcounty",
{{ validate_date("date_facility_opened") }} AS "date_facility_opened",
"facility_id",
"facility_name",
"facility_type",
{{ validate_date("date_of_submission") }} AS "date_of_submission",
"status_of_facility",
"case_name",
"case_type",
{{ validate_date("date_opened") }} AS "date_opened",
"case_id",
{{ validate_date("indexed_on") }} AS "indexed_on",
"closed",
{{ validate_date("created_at") }} AS "created_at",
CASE
        WHEN NULLIF("date_of_submission", '') IS NOT NULL 
        THEN EXTRACT(YEAR FROM CAST(NULLIF("date_of_submission", '') AS DATE))
        ELSE NULL
    END AS "year"
FROM {{ ref('staging_facilities') }}
WHERE "facility_name" != 'test'