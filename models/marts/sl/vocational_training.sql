{{ config(materialized='table', tags=['sl', 'sl_marts']) }}

select 
    case_id,
    date_of_registration,
    pp_unique_id,
    pp_fullname,
    'TVET' as service,
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
    name_of_institution_tvet,
    name_of_facilitator,
    course_enrolled_tvet,
    start_date_tvet,
    nita_exams,
    how_helpful_course_tvet,
    recommend_training_tvet,
    location_of_institution_ttia_raw,
    tvet_latitude,
    tvet_longitude,
    tvet_altitude,
    tvet_accuracy
from {{ ref('staging_sl_case_table') }}
where (name_of_institution_tvet is not null and trim(name_of_institution_tvet) != '')
   or (course_enrolled_tvet is not null and trim(course_enrolled_tvet) != '')
   or start_date_tvet is not null
