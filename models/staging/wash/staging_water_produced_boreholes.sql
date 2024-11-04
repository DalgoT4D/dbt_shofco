{{ config(
  materialized='table'
) }}

WITH water_produced_data AS (
    SELECT
        id,
        data::json->'form'->'case'->'@case_id' AS case_id,
        data::json->'form'->>'amount_produced' AS amount_produced,
        data::json->'form'->>'date_of_data_entry' AS date_of_data_entry,
        data::json->'form'->>'date_of_submission' AS date_of_submission,
        data::json->'form'->>'week_of_submission' AS week_of_submission,
        data::json->'form'->'subcase_0'->'case'->'update'->>'date_of_data_entry' AS subcase_date_of_data_entry,
        data::json->'form'->'subcase_0'->'case'->'update'->>'amount_produced' AS subcase_amount_produced
    FROM {{ source('wash_staging', 'Water_Produced_at_Boreholes') }}
)

SELECT
    id,
    case_id,
    amount_produced,
    CAST(date_of_data_entry AS DATE) AS date_of_data_entry,
    CAST(date_of_submission AS DATE) AS date_of_submission,
    week_of_submission,
    CAST(subcase_date_of_data_entry AS DATE) AS subcase_date_of_data_entry,
    subcase_amount_produced
FROM water_produced_data
