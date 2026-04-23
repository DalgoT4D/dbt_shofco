{{ config(materialized='table', tags=['sl', 'sl_marts', 'sl_locations']) }}

select
    case_id,
    gps_of_business_location_apbl_raw          as gps_raw,
    apprenticeship_latitude                    as latitude,
    apprenticeship_longitude                   as longitude,
    apprenticeship_altitude                    as altitude,
    apprenticeship_accuracy                    as accuracy
from {{ ref('staging_sl_case_table') }}
where apprenticeship_latitude  is not null
  and apprenticeship_longitude is not null
  and apprenticeship_latitude  <> 0
  and apprenticeship_longitude <> 0