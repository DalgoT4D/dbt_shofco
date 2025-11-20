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
    digital_literacy_dl,
    coalesce(start_date_dl, advanced_it_start_date_dl) as digital_literacy_start_date,
    coalesce(completion_date_dl, advanced_it_completion_date_dl) as digital_literacy_end_date,
    final_exams_tc
from {{ ref('staging_sl_case_table') }}
where digital_literacy_dl is not null
  and trim(digital_literacy_dl) != ''
