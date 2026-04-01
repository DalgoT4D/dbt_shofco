{{ config(materialized='table', tags=['sl', 'sl_marts', 'sl_locations']) }}

select
    case_id,
    mark_the_location_of_the_business_raw      as gps_raw,
    business_latitude                          as latitude,
    business_longitude                         as longitude,
    business_altitude                          as altitude,
    business_accuracy                          as accuracy
from {{ ref('staging_sl_case_table') }}
where business_latitude  is not null
  and business_longitude is not null
  and business_latitude  <> 0
  and business_longitude <> 0