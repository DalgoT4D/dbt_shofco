{{ config(
  materialized='table'
) }}

WITH base_data AS (
    SELECT
        "id",
        "case_id",
        "amount_produced",
        "date_of_data_entry",
        "date_of_submission",
        "week_of_submission",
        "subcase_date_of_data_entry",
        "subcase_amount_produced"
    FROM {{ ref('staging_water_produced_boreholes') }}
)

-- Generate the final dataset, keeping both original and subcase data
SELECT
    "id",
    "case_id",
    "amount_produced",  -- Original amount produced
    "date_of_data_entry",  -- Original date of data entry
    "date_of_submission",  -- Same date of submission
    "week_of_submission"   -- Same week of submission
FROM base_data

UNION ALL

SELECT
    "id",
    "case_id",
    "subcase_amount_produced" AS "amount_produced",  -- Use the amount from subcase
    "subcase_date_of_data_entry" AS "date_of_data_entry",  -- Use the date of data entry from subcase
    "date_of_submission",  -- Keep the same date of submission
    "week_of_submission"   -- Keep the same week of submission
FROM base_data
WHERE "subcase_date_of_data_entry" IS NOT NULL AND "subcase_amount_produced" IS NOT NULL
