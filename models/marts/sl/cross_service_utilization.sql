{{ config(materialized='table', tags=['sl', 'sl_marts', 'cross_service']) }}

select
    case_id,
    pp_unique_id,
    pp_fullname,
    gender,
    county,
    date_of_registration,
    is_pwd,
    is_young_mother,
    
    -- service participation flags
    (digital_literacy_dl is not null or start_date_dl is not null 
     or advanced_it_start_date_dl is not null or completion_date_dl is not null) as participated_digital_literacy,
    
    (date_first_visit_bm is not null or date_of_second_visit_bm is not null) as participated_mentorship,
    
    (apprenticeship_provider_apr is not null or placement_date_apr is not null) as participated_apprenticeship,
    
    (start_date_int is not null or completion_date_int is not null) as participated_internship,
    
    (swep_handcraft_center_hc is not null or start_date_hc is not null) as participated_handcraft,
    
    (swep_iga_center_iga is not null or start_date_iga is not null) as participated_iga,
    
    (swep_tailoring_center_st is not null or start_date_of_training_st is not null) as participated_tailoring,
    
    (name_of_institution_tvet is not null or start_date_tvet is not null) as participated_tvet,
    
    (grant_amount_bg is not null or date_grant_allocated_bg is not null) as participated_business_grants,
    
    (start_date_ent is not null or completion_date_ent is not null) as participated_entrepreneurship,
    
    (date_of_visit_csf is not null or date_of_visit_csr is not null) as participated_coworking,
    
    (income_on_average_pl is not null or placement_opportunity_pl is not null) as participated_job_placements,
    
    -- total service count
    (case when digital_literacy_dl is not null or start_date_dl is not null then 1 else 0 end +
     case when date_first_visit_bm is not null or date_of_second_visit_bm is not null then 1 else 0 end +
     case when apprenticeship_provider_apr is not null or placement_date_apr is not null then 1 else 0 end +
     case when start_date_int is not null or completion_date_int is not null then 1 else 0 end +
     case when swep_handcraft_center_hc is not null or start_date_hc is not null then 1 else 0 end +
     case when swep_iga_center_iga is not null or start_date_iga is not null then 1 else 0 end +
     case when swep_tailoring_center_st is not null then 1 else 0 end +
     case when name_of_institution_tvet is not null or start_date_tvet is not null then 1 else 0 end +
     case when grant_amount_bg is not null or date_grant_allocated_bg is not null then 1 else 0 end +
     case when start_date_ent is not null or completion_date_ent is not null then 1 else 0 end +
     case when date_of_visit_csf is not null or date_of_visit_csr is not null then 1 else 0 end +
     case when income_on_average_pl is not null or placement_opportunity_pl is not null then 1 else 0 end) as total_services_count,
    
    -- service combination
    concat_ws(' + ',
        case when digital_literacy_dl is not null or start_date_dl is not null then 'Digital Literacy' end,
        case when date_first_visit_bm is not null or date_of_second_visit_bm is not null then 'Mentorship' end,
        case when apprenticeship_provider_apr is not null or placement_date_apr is not null then 'Apprenticeship' end,
        case when start_date_int is not null or completion_date_int is not null then 'Internship' end,
        case when swep_handcraft_center_hc is not null or start_date_hc is not null then 'Handcraft' end,
        case when swep_iga_center_iga is not null or start_date_iga is not null then 'IGA' end,
        case when swep_tailoring_center_st is not null then 'Tailoring' end,
        case when name_of_institution_tvet is not null or start_date_tvet is not null then 'TVET' end,
        case when grant_amount_bg is not null or date_grant_allocated_bg is not null then 'Business Grants' end,
        case when start_date_ent is not null or completion_date_ent is not null then 'Entrepreneurship' end,
        case when date_of_visit_csf is not null or date_of_visit_csr is not null then 'Coworking' end,
        case when income_on_average_pl is not null or placement_opportunity_pl is not null then 'Job Placements' end
    ) as service_combo

from {{ ref('staging_sl_case_table') }}
order by case_id

