{{ config(materialized='table', tags=['sl_new_models']) }}

select 
    case_id,
    date_of_registration,
    pp_unique_id,
    pp_fullname,
    gender,
    nationality,
    kenyan_national_id_number_dir,
    county,
    subcounty,
    ward,
    primary_phone_number,
    phone_last_8_digits,
    is_pwd,
    grant_amount_bg,
    date_grant_allocated_bg
from {{ ref('staging_sl_case_table') }}
where grant_amount_bg is not null
  and trim(grant_amount_bg) != ''
