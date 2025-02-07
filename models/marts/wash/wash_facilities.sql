{{ config(
  materialized='table',
  tags=['wash_facilities',"wash"]
) }}

select
    ward,
    county,
    village,
    subcounty,
    date_facility_opened,
    facility_id,
    facility_name,
    facility_type,
    date_of_submission,
    status_of_facility,
    case_name,
    case_type,
    date_opened,
    case_id,
    indexed_on,
    closed,
    created_at,
    REGEXP_REPLACE(latitude, '[^0-9.-]', '', 'g') as latitude,
    REGEXP_REPLACE(longitude, '[^0-9.-]', '', 'g') as longitude,
    case
        when NULLIF(date_of_submission, '') is not NULL
            then
                EXTRACT(year from CAST(NULLIF(date_of_submission, '') as DATE))
    end as year
from {{ ref('staging_facilities') }}
where facility_name != 'test'
