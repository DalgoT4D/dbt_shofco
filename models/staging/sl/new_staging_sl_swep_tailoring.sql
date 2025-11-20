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
    swep_tailoring_center_st,
    start_date_of_training_st,
    expected_end_date_of_training_st,
    training_session_st,
    nita_exams
from {{ ref('staging_sl_case_table') }}
where (swep_tailoring_center_st is not null and trim(swep_tailoring_center_st) != '')
   or start_date_of_training_st is not null
   or expected_end_date_of_training_st is not null
   or (training_session_st is not null and trim(training_session_st) != '')
