{{ config(
  materialized='table'
) }}

SELECT
    *
FROM {{ ref("staging_public_partnerships") }}