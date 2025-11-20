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
    name_of_institution_tvet,
    course_enrolled_tvet,
    start_date_tvet,
    nita_exams
from {{ ref('staging_sl_case_table') }}
where (name_of_institution_tvet is not null and trim(name_of_institution_tvet) != '')
   or (course_enrolled_tvet is not null and trim(course_enrolled_tvet) != '')
   or start_date_tvet is not null
