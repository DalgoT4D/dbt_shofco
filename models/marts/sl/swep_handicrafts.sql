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
    swep_handcraft_center_hc,
    start_date_hc,
    completion_date_hc,
    handcraft_training_type_hc
from {{ ref('staging_sl_case_table') }}
where (swep_handcraft_center_hc is not null and trim(swep_handcraft_center_hc) != '')
   or start_date_hc is not null
   or completion_date_hc is not null
   or (handcraft_training_type_hc is not null and trim(handcraft_training_type_hc) != '')

