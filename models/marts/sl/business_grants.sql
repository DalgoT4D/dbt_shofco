{{ config(materialized='table', tags=['sl', 'sl_marts']) }}

select
    case_id,
    date_of_registration,
    pp_unique_id,
    pp_fullname,
    'Business Grants' as service,
    gender,
    nationality,
    refugee_type,
    kenyan_national_id_number_dir,
    county,
    subcounty,
    ward,
    primary_phone_number,
    phone_last_8_digits,
    is_pwd,
    type_of_disability_dir,
    is_young_mother,
    grant_amount_bg,
    date_grant_allocated_bg,
    type_of_business_you_operate_bga
from {{ ref('staging_sl_case_table') }}
where grant_amount_bg is not null
  and trim(grant_amount_bg) != ''