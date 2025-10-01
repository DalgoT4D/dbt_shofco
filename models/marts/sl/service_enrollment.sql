{{ config(
  materialized='table',
  tags=["service_enrollment", "sl", 'daycare', 'dignity_kit', "sl_marts"]
) }}

SELECT
  case_id,
  pp_fullname AS full_name,
  sex AS gender,  -- Using coalesced field
  CASE WHEN age ~ '^[0-9]+$' THEN age::INT ELSE NULL END AS age,  -- Using coalesced field
  pp_unique_id AS unique_id,
  course_enrolled_sr AS enrolled_course,
  ongoing_course AS ongoing_course,
  county AS county,
  pp_shofco_subcounty AS subcounty,
  pp_ahofco_ward AS ward,
  participants_program_sr AS program,
  is_existing_participant,
  previous_service_completed,
  skills_before_joining_shofco_sr,

  /* location/enumerator */
  user_location_id AS location_id,
  enumerator_first_Name AS enumerator_firstname,  -- Using coalesced field
  enumerator_last_name AS enumerator_lastname,  -- Using coalesced field
  enumerator AS enumerator,


  /* transitions */
  employment_type_sr AS transition_employment_type,
  preferred_region_sr AS transition_preferred_region,
  computer_literate_sr AS transition_computer_literate,
  willingness_to_relocate_sr AS transition_willingness_to_relocate,
  job_placement_support_needed_sr AS transition_job_placement_support_needed,
  post_training_support_needed_sr AS transition_post_training_support_needed,
  preferred_sectors_for_employment_sr AS transition_preferred_sectors,
  support_to_help_succeed_as_entrepreneur_sr AS transition_support_entrepreneur,

  /* type of work */
  participants_type_of_work_sr,
  number_of_employees_sr AS employees_sr,
  type_of_business_sr,
  business_location,  -- Using coalesced field
  year_business_started_sr,
  monthly_business_income_sr,

  /* internship */
  internship_position_int AS internship_position,
  name_of_organization_int AS internship_org,
  supervisors_name_int AS internship_supervisor,
  Supervisor_phone_number_int AS internship_supervisor_phone,
  {{ validate_date('start_date_int') }} AS internship_start_date,
  {{ validate_date('completion_date_int') }} AS internship_completion_date,

  /* tailoring */
  swep_tailoring_center_st AS tailoring_center,
  {{ validate_date('start_date_of_training_st') }} AS tailoring_start_date,
  {{ validate_date('expected_end_date_of_training_st') }} AS tailoring_expected_end,

  /* digital literacy */
  digital_literacy_dl AS digital_literacy_course,
  {{ validate_date('start_date_dl') }} AS digital_literacy_start_date,
  {{ validate_date('completion_date_dl') }} AS digital_literacy_completion_date,
  digital_literacy_center_dl AS digital_literacy_center,
  specify_other_dl AS digital_literacy_other_training,
  time_of_training_dl AS time_of_training,
  computer_packages_start_date_dl AS computer_packages_start_date,
  computer_packages_completion_date_dl AS computer_packages_completion_date,
  advanced_it_dl,
  advanced_it_start_date_dl AS advanced_it_start_date,
  advanced_it_completion_date_dl AS advanced_it_completion_date,

  /* entrepreneurship */
  {{ validate_date('start_date_ent') }} AS entrepreneurship_start_date,
  {{ validate_date('completion_date_ent') }} AS entrepreneurship_completion_date,
  interest_in_sales_work_ent AS entrepreneurship_interest_in_sales,

  /* tvet */
  {{ validate_date('start_date_tvet') }} AS tvet_start_date,
  {{ validate_date('completion_date_tvet') }} AS tvet_completion_date,
  course_enrolled_tvet AS tvet_course,
  guardian_alternative_phone_number_tvet AS tvet_guardian_alt_phone,
  name_of_institution_tvet AS tvet_institution,
  parent_name_tvet AS tvet_parent_name,
  parent_phone_number_tvet AS tvet_parent_phone,
  county_placed_tvet AS tvet_county_placed,

  /* handcraft */
  handcraft_training_type_hc AS handcraft_training_type,
  {{ validate_date('start_date_hc') }} AS handcraft_start_date,
  {{ validate_date('completion_date_hc') }} AS handcraft_completion_date,
  NULL AS handcraft_follow_up_flag,

  /* iga */
  training_type_iga AS iga_training_type,
  {{ validate_date('start_date_iga') }} AS iga_start_date,
  {{ validate_date('Completion_date_iga') }} AS iga_completion_date,
  swep_iga_center_iga AS iga_center,
  interest_in_sales_work_iga AS iga_interest_in_sales,

  /* apprenticeship */
  skill_enrolled_apr AS apprenticeship_skill,
  {{ validate_date('placement_date_apr') }} AS apprenticeship_placement_date,
  {{ validate_date('completion_date_apr') }} AS apprenticeship_completion_date,
  guardians_name_apr AS apprenticeship_guardian_name,
  guardians_phone_number_apr AS apprenticeship_guardian_phone,
  NULL AS apprenticeship_daycare_support,

  /* business mentorship */
  region AS bm_region,  -- Using coalesced field
  county AS bm_county, -- Using coalesced county field instead of county_bm
  employees_bm AS bm_employees,
  bus_location_bm AS bm_business_location,
  name_of_mentor_bm AS bm_mentor_name,
  contact_of_mentor_bm AS bm_mentor_contact,  -- Using coalesced field
  type_of_business_bm AS type_of_business,
  stock_bm AS bm_stock,
  {{ validate_date('date_first_visit_bm') }} AS bm_first_visit_date,
  cash_to_buy_new_stock_bm AS bm_cash_to_buy_stock,
  mentee_business_records_bm AS bm_business_records,
  if_mentee_have_dead_stock_bm AS bm_dead_stock,
  challenges_mentee_is_facing_bm AS bm_challenges_facing,
  solutions_agreed_to_be_implemented_bm AS bm_solutions,
  business_costs_before_setting_prices_bm AS bm_business_costs_before_price,
  mentees_price_compared_to_competitors_bm AS bm_price_comparison,
  agreed_timeline_to_implement_solutions_bm AS bm_solutions_timeline

FROM {{ ref('staging_sl_case_table') }}
WHERE case_type = 'sl_new'