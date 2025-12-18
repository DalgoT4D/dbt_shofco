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
    start_date_int,
    completion_date_int
from {{ ref('staging_sl_case_table') }}
where start_date_int is not null
   or completion_date_int is not null

