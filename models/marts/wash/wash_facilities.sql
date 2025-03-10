{{ config(
  materialized='table',
  tags=['wash_facilities',"wash"]
) }}

select
    ward,
    county,
    village,
    REGEXP_REPLACE(latitude, '[^0-9.-]', '') as latitude, 
    REGEXP_REPLACE(longitude, '[^0-9.-]', '') as longitude, 
    subcounty,
    {{ validate_date("date_facility_opened") }} as date_facility_opened,
    facility_id,
    facility_name,
    facility_type,
    {{ validate_date("date_of_submission") }} as date_of_submission,
    status_of_facility,
    case_name,
    case_type,
    {{ validate_date("date_opened") }} as date_opened,
    case_id,
    {{ validate_date("indexed_on") }} as indexed_on,
    closed,
    {{ validate_date("created_at") }} as created_at,
    case
        when NULLIF(date_of_submission, '') is not NULL 
            then EXTRACT(year from CAST(NULLIF(date_of_submission, '') as DATE))
    end as year
from {{ ref('staging_facilities') }}
where facility_name != 'test'
