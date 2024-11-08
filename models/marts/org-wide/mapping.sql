{{ config(
  materialized='table'
) }}

SELECT
    *,
    
    -- Extract GPS components from gps_location field
    split_part("gps_location", ' ', 1)::float AS "latitude",
    split_part("gps_location", ' ', 2)::float AS "longitude",
    split_part("gps_location", ' ', 3)::float AS "altitude",
    split_part("gps_location", ' ', 4)::float AS "accuracy"
    
FROM {{ ref('staging_mapping') }}