{{ config(materialized='table', tags=['sl', 'sl_marts', 'sl_locations']) }}

select
    case_id,
    location_of_institution_ttia_raw           as gps_raw,
    tvet_latitude                              as latitude,
    tvet_longitude                             as longitude,
    tvet_altitude                              as altitude,
    tvet_accuracy                              as accuracy
from {{ ref('staging_sl_case_table') }}
where tvet_latitude  is not null
  and tvet_longitude is not null
  and tvet_latitude  <> 0
  and tvet_longitude <> 0