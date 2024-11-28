{{ config(
  materialized='table'
) }}

SELECT
    *
FROM {{ ref("staging_followup_attendance") }}