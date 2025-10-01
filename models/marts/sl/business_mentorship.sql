{{ 
  config(
    materialized='table',
    tags=['business_mentorship', 'sl', 'mentorship', "sl_marts"]
  ) 
}}

SELECT
    case_id,
    user_id,
    pp_fullname AS mentee_full_name,
    pp_unique_id AS mentee_unique_id,
    
    -- Mentee Demographics
    CASE WHEN age ~ '^[0-9]+$' THEN age::INT ELSE NULL END AS mentee_age,
    sex AS mentee_gender,
    date_of_birth AS mentee_date_of_birth,
    national_id_no_bm AS mentee_national_id,
    nationality_of_the_mentee_bm AS mentee_nationality,
    are_you_a_person_living_with_disability_bm AS mentee_has_disability,
    
    -- Contact Information  
    contact_of_the_mentee_bm AS mentee_contact,
    alternative_contact_bm AS mentee_alternative_contact,
    
    -- Location Information
    mentorship_county_bm AS mentorship_county,
    mentorship_subcounty_bm AS mentorship_subcounty, 
    mentorship_ward_bm AS mentorship_ward,
    county AS mentee_county,
    subcounty_bm AS mentee_subcounty,
    ward_bm AS mentee_ward,
    region_of_the_mentorship_bm AS mentorship_region,
    
    -- Mentor Information
    name_of_mentor_bm AS mentor_name,
    name_of_the_mentor_bm AS mentor_name_alt,
    contact_of_mentor_bm AS mentor_contact,
    
    -- Business Information
    name_of_the_mentee_bm AS business_mentee_name,
    type_of_business_bm AS business_type,
    business_location AS business_location,
    bus_location_bm AS business_location_bm,
    
    -- Business Performance & Stock
    stock_bm AS current_stock,
    other_stock_bm AS other_stock_details,
    how_often_mentee_buys_stock_bm AS stock_purchase_frequency,
    cash_to_buy_new_stock_bm AS cash_available_for_stock,
    if_mentee_always_have_the_cash_to_buy_new_stock_bm AS always_has_cash_for_stock,
    if_mentee_have_dead_stock_bm AS has_dead_stock,
    
    -- Business Management
    business_records_the_mentee_keeps_bm AS business_records_kept,
    mentee_business_records_bm AS mentee_keeps_records,
    business_costs_before_setting_prices_bm AS calculates_costs_before_pricing,
    mentees_price_compared_to_competitors_bm AS price_vs_competitors,
    
    -- Employees
    employees_bm AS has_employees,
    how_many_employees_bm AS number_of_employees,
    female_employees_bm AS number_of_female_employees,
    
    -- Visit Information
    visit_bm AS visit_number,
    date_first_visit_bm AS first_visit_date,
    date_of_first_visit_bm AS first_visit_date_alt,
    date_of_second_visit_bm AS second_visit_date,
    date_of_the_second_visit_bm AS second_visit_date_alt,
    
    -- Challenges & Solutions
    challenges_mentee_is_facing_bm AS challenges_facing,
    challenges_mentee_is_facing_running_business_bm AS business_challenges,
    solutions_agreed_to_be_implemented_bm AS agreed_solutions,
    agreed_timeline_to_implement_solutions_bm AS implementation_timeline,
    agreed_timeline_to_implement_the_solutions_bm AS implementation_timeline_alt,
    output_of_the_agreed_implemented_activities_bm AS implemented_activities_output,
    
    -- Mentorship Outcomes
    areas_the_mentee_needs_further_sessions_bm AS areas_needing_more_sessions,
    more_mentorship_sessions_bm AS needs_more_sessions,
    whether_the_mentee_needs_more_mentorship_sessions_bm AS needs_additional_mentorship,
    final_business_recomendation_to_mentee_bm AS final_recommendation,
    wayforward_of_the_business_bm AS business_way_forward,
    
    -- Additional Details
    elaborate_bm AS additional_elaboration,
    
    -- Metadata
    enumerator AS enumerator_name,
    enumerator_first_Name AS enumerator_first_name,
    enumerator_last_name AS enumerator_last_name,
    indexed_on AS date_received,
    date_modified AS last_modified

FROM {{ ref('staging_sl_case_table') }}
WHERE 
    -- Filter for business mentorship records
    (name_of_mentor_bm IS NOT NULL 
     OR mentorship_county_bm IS NOT NULL 
     OR type_of_business_bm IS NOT NULL
     OR visit_bm IS NOT NULL
     OR challenges_mentee_is_facing_bm IS NOT NULL)