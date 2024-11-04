{{ config(
  materialized='table'
) }}

-- Extract relevant fields from JSON data in Water Facility Table
SELECT
    "id",
    data::json->'form'->'case'->'@case_id' AS "case_id",
    data::json->'form'->'case'->'create'->>'case_name' AS "case_name",
    data::json->'form'->'case'->'update'->>'ward' AS "ward",
    data::json->'form'->'case'->'update'->>'county' AS "county",
    data::json->'form'->'case'->'update'->>'village' AS "village",
    data::json->'form'->'case'->'update'->>'latitude' AS "latitude",
    data::json->'form'->'case'->'update'->>'longitude' AS "longitude",
    data::json->'form'->'case'->'update'->>'subcounty' AS "subcounty",
    data::json->'form'->'case'->'update'->>'facility_id' AS "facility_id",
    data::json->'form'->'case'->'update'->>'facility_name' AS "facility_name",
    data::json->'form'->'case'->'update'->>'facility_type' AS "facility_type",
    data::json->'form'->'case'->'update'->>'date_of_submission' AS "date_of_submission",
    data::json->'form'->'case'->'update'->>'status_of_facility' AS "status_of_facility"
FROM {{ source('wash_staging', 'Add_Facility') }}