{{ config(
  materialized='table'
) }}

SELECT
    *
FROM {{ ref("staging_parents_attendance") }}