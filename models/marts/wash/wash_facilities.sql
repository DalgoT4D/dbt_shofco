{{ config(
  materialized='table'
) }}

select
*
FROM {{ ref('facilities') }}