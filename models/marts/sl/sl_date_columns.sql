{{ config(materialized='table', tags=['sl_marts', 'date_analysis', 'sl']) }}

select
    case_id,
    pp_unique_id,
    pp_fullname,
    gender,
    county,
    subcounty,
    ward,
    nationality,
    is_pwd,
    is_young_mother,
    
    -- Registration dates
    date_of_registration,
    date_of_birth_csf,
    
    -- Apprenticeship dates
    placement_date_apr,
    
    -- Business grants dates
    date_grant_allocated_bg,
    
    -- Digital literacy dates (merged)
    coalesce(start_date_dl, advanced_it_start_date_dl) as digital_literacy_start_date,
    coalesce(completion_date_dl, advanced_it_completion_date_dl) as digital_literacy_end_date,
    advanced_it_start_date_dl,
    advanced_it_completion_date_dl,
    completion_date_dl,
    
    -- Entrepreneurship dates
    start_date_ent,
    completion_date_ent,
    
    -- Internship dates
    start_date_int,
    completion_date_int,
    
    -- Business mentorship visit dates
    date_first_visit_bm,
    date_of_second_visit_bm,
    
    -- Coworking space visit dates
    date_of_visit_csf,
    date_of_visit_csr,
    
    -- SWEP Handicrafts dates
    start_date_hc,
    completion_date_hc,
    
    -- SWEP IGA dates
    start_date_iga,
    completion_date_iga,
    
    -- SWEP Tailoring dates
    start_date_of_training_st,
    expected_end_date_of_training_st,
    
    -- TVET dates
    start_date_tvet,
    completion_date_tvet

from {{ ref('staging_sl_case_table') }}
order by case_id
