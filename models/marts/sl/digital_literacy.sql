{{ config(materialized='table', tags=['sl', 'sl_marts']) }}

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
    is_young_mother,
    digital_literacy_dl,
    coalesce(start_date_dl, advanced_it_start_date_dl) as digital_literacy_start_date,
    coalesce(completion_date_dl, advanced_it_completion_date_dl) as digital_literacy_end_date,
    final_exams_tc,
    how_helpful_course_tc,
    why_not_helpful_tc,
    how_helpful_was_upskilling_uc,
    why_not_helpful_uc
from {{ ref('staging_sl_case_table') }}
where digital_literacy_dl is not null
  and trim(digital_literacy_dl) != ''
