{{ config(
  materialized='table',
  tags='wash_facilities'
) }}

select
*
FROM {{ ref('facilities') }}