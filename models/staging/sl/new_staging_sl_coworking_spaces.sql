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
    date_of_visit_csf,
    date_of_visit_csr,
    how_many_csf,
    time_in_csf,
    time_in_csr,
    time_out_csf,
    time_out_csr,
    using_coworking_for_a_living_csr,
    using_coworking_to_earn_a_living_csf,
    what_visitor_is_using_csf,
    what_visitor_is_using_csr,
    purpose_for_using_csf,
    coworking_space_facility
from {{ ref('staging_sl_case_table') }}
where date_of_visit_csf is not null
   or date_of_visit_csr is not null
   or (coworking_space_facility is not null and trim(coworking_space_facility) != '')
   or (using_coworking_to_earn_a_living_csf is not null and trim(using_coworking_to_earn_a_living_csf) != '')
   or (using_coworking_for_a_living_csr is not null and trim(using_coworking_for_a_living_csr) != '')
   or (what_visitor_is_using_csf is not null and trim(what_visitor_is_using_csf) != '')
   or (what_visitor_is_using_csr is not null and trim(what_visitor_is_using_csr) != '')
   or (purpose_for_using_csf is not null and trim(purpose_for_using_csf) != '')
