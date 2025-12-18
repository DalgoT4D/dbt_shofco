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
    swep_iga_center_iga,
    start_date_iga,
    completion_date_iga,
    training_type_iga
from {{ ref('staging_sl_case_table') }}
where (swep_iga_center_iga is not null and trim(swep_iga_center_iga) != '')
   or start_date_iga is not null
   or completion_date_iga is not null
   or (training_type_iga is not null and trim(training_type_iga) != '')

