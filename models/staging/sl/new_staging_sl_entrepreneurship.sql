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
    start_date_ent,
    completion_date_ent
from {{ ref('staging_sl_case_table') }}
where start_date_ent is not null
   or completion_date_ent is not null
