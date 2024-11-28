{{ config(
  materialized='table'
) }}

SELECT
    *
FROM {{ ref("staging_students_attendance") }}