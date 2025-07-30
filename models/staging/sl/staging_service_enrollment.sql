{{ config(
  materialized="table",
  unique_key="case_id",
  tags=["service_enrollment","sl"]
) }}

with src as (
  select
    id as case_id,
    data::jsonb as d
  from {{ source('staging_sl', 'service_enroll') }}
  WHERE
        data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
),

staged as (
  select
    case_id,

    /* meta_data */
    d->'form'->'enrollment_sr'->'meta_data'->> 'pp_fullname'            as pp_fullname,
    d->'form'->'enrollment_sr'->'meta_data'->> 'pp_sex'                 as pp_sex,
    d->'form'->'enrollment_sr'->'meta_data'->> 'pp_age'                 as pp_age,
    d->'form'->'enrollment_sr'->> 'is_completed'                        as is_completed,
    d->'form'->'enrollment_sr'->'meta_data'->> 'pp_unique_id'           as pp_unique_id,
    d->'form'->'enrollment_sr'->'meta_data'->> 'ongoing_course'         as ongoing_course,
    d->'form'->'enrollment_sr'->> 'course_enrolled_sr'                  as course_enrolled_sr,
    -- d->'form'->'enrollment_sr'->'meta_data'->> 'pp_shofco_county'       as pp_shofco_county,
coalesce(
  nullif(trim(d->'form'->'enrollment_sr'->'meta_data'->> 'pp_shofco_county'), ''),
  nullif(trim(d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'mentors_details'->> 'county_bm'), ''),
  nullif(trim(d->'form'->'case'->'update'->> 'county_bm'), '')
) as  pp_shofco_county,

coalesce(
  nullif(trim(d->'form'->'enrollment_sr'->'meta_data'->> 'pp_shofco_subcounty'), ''),
  nullif(trim(d->'form'->'case'->'update'->> 'pp_shofco_subcounty'), '')
) as pp_shofco_subcounty,

coalesce(
  nullif(trim(d->'form'->'enrollment_sr'->'meta_data'->> 'pp_ahofco_ward'), ''),
  nullif(trim(d->'form'->'case'->'update'->> 'pp_ahofco_ward'), '')
) as pp_shofco_ward,


    d->'form'->'enrollment_sr'->'meta_data'->> 'user_location_id'       as user_location_id,
    d->'form'->'enrollment_sr'->'meta_data'->> 'enumerators_first'      as enumerators_first,
    d->'form'->'enrollment_sr'->'meta_data'->> 'enumerator_last-name'   as enumerator_last_name,
    d->'form'->'enrollment_sr'->'meta_data'->> 'enumerator'             as enumerator_full_name,

    /* general sr fields */
    d->'form'->'enrollment_sr'->> 'participants_program_sr'            as participants_program_sr,
    d->'form'->'enrollment_sr'->> 'is_existing_participant'            as is_existing_participant,
    d->'form'->'enrollment_sr'->> 'previous_service_completed'         as previous_service_completed,
    d->'form'->'enrollment_sr'->> 'skills_before_joining_shofco_sr'    as skills_before_joining_shofco_sr,

    /* type_of_work_sr block */
    d->'form'->'enrollment_sr'->'type_of_work_sr'->> 'participants_type_of_work_sr' as participants_type_of_work_sr,
    d->'form'->'enrollment_sr'->'type_of_work_sr'->> 'employees_sr'         as employees_sr,
    d->'form'->'enrollment_sr'->'type_of_work_sr'->> 'type_of_business_sr'   as type_of_business_sr,
    d->'form'->'enrollment_sr'->'type_of_work_sr'->> 'business_location_sr' as business_location_sr,
    d->'form'->'enrollment_sr'->'type_of_work_sr'->> 'year_business_started_sr'    as year_business_started_sr,
    d->'form'->'enrollment_sr'->'type_of_work_sr'->> 'monthly_business_income_sr'   as monthly_business_income_sr,

    /* daycare_support_sr */
    d->'form'->'enrollment_sr'->'daycare_support_sr'->> 'children_under_4_sr'                  as daycare_children_under_4,
    d->'form'->'enrollment_sr'->'daycare_support_sr'->> 'number_of_children_sr'               as daycare_number_of_children,
    d->'form'->'enrollment_sr'->'daycare_support_sr'->> 'do_you_have_children_sr'             as daycare_do_you_have_children,
    d->'form'->'enrollment_sr'->'daycare_support_sr'->> 'any_children_with_disabilities_sr'  as daycare_children_with_disabilities,
    d->'form'->'enrollment_sr'->'daycare_support_sr'->> 'daycare_support_details'              as daycare_details,

    /* dignity_kit_support_sr */
    d->'form'->'enrollment_sr'->'dignity_kit_support_sr'->> 'dignity_kit_support'                       as dignity_kit_support,
    d->'form'->'enrollment_sr'->'dignity_kit_support_sr'->> 'experienced_days_where_you_had_nothing_sr' as dignity_experienced_days_nothing,
    d->'form'->'enrollment_sr'->'dignity_kit_support_sr'->> 'miss_school_due_to_lack_hygiene_products_sr' as dignity_miss_school,
    d->'form'->'enrollment_sr'->'dignity_kit_support_sr'->> 'challenges_accessing_personal_hygiene_items_sr' as dignity_challenges_hygiene,

    /* desired_transition_sr */
    d->'form'->'enrollment_sr'->'desired_transition_sr'->> 'employment_type_sr'                  as transition_employment_type,
    d->'form'->'enrollment_sr'->'desired_transition_sr'->> 'preferred_region_sr'                   as transition_preferred_region,
    d->'form'->'enrollment_sr'->'desired_transition_sr'->> 'computer_literate_sr'                  as transition_computer_literate,
    d->'form'->'enrollment_sr'->'desired_transition_sr'->> 'willingness_to_relocate_sr'           as transition_willingness_to_relocate,
    d->'form'->'enrollment_sr'->'desired_transition_sr'->> 'job_placement_support_needed_sr'     as transition_job_placement_support_needed,
    d->'form'->'enrollment_sr'->'desired_transition_sr'->> 'post_training_support_needed_sr'     as transition_post_training_support_needed,
    d->'form'->'enrollment_sr'->'desired_transition_sr'->> 'preferred_sectors_for_employment_sr' as transition_preferred_sectors,
    d->'form'->'enrollment_sr'->'desired_transition_sr'->> 'support_to_help_succeed_as_entrepreneur_sr' as transition_support_entrepreneur,

    /* course specific blocks */

    /* --- internship_followup_questions/internship_placement_details */
    d->'form'->'enrollment_sr'->'internship_followup_questions'->'internship_placement_details'->> 'internship_position_int'       as internship_position,
    d->'form'->'enrollment_sr'->'internship_followup_questions'->'internship_placement_details'->> 'name_of_organization_int'      as internship_org,
    d->'form'->'enrollment_sr'->'internship_followup_questions'->'internship_placement_details'->> 'supervisors_name_int'          as internship_supervisor,
    d->'form'->'enrollment_sr'->'internship_followup_questions'->'internship_placement_details'->> 'Supervisor_phone_number_int'  as internship_supervisor_phone,
    d->'form'->'enrollment_sr'->'internship_followup_questions'->'internship_placement_details'->> 'start_date_int'                as internship_start_date,
    d->'form'->'enrollment_sr'->'internship_followup_questions'->'internship_placement_details'->> 'completion_date_int'           as internship_completion_date,

    /* --- sw_ep_tailoring questions */
    d->'form'->'enrollment_sr'->'swep_tailoring_questions_sr'->> 'swep_tailoring_center_st'              as tailoring_center,
    d->'form'->'enrollment_sr'->'swep_tailoring_questions_sr'->> 'start_date_of_training_st'            as tailoring_start_date,
    d->'form'->'enrollment_sr'->'swep_tailoring_questions_sr'->> 'expected_end_date_of_training_st'    as tailoring_expected_end,

    /* --- digital_literacy_follow_up_sr */
    d->'form'->'enrollment_sr'->'digital_literacy_follow_up_sr'->> 'start_date_dl'            as digital_literacy_start_date,
    d->'form'->'enrollment_sr'->'digital_literacy_follow_up_sr'->> 'completion_date_dl'       as digital_literacy_completion_date,
    d->'form'->'enrollment_sr'->'digital_literacy_follow_up_sr'->> 'digital_literacy_dl'      as digital_literacy_course,
    d->'form'->'enrollment_sr'->'digital_literacy_follow_up_sr'->> 'specify_other_dl'            as digital_literacy_other_training,
    d->'form'->'enrollment_sr'->'digital_literacy_follow_up_sr'->> 'advanced_it_dl'       as advanced_it_dl,
    d->'form'->'enrollment_sr'->'digital_literacy_follow_up_sr'->> 'advanced_it_start_date_dl'      as advanced_it_start_date,
    d->'form'->'enrollment_sr'->'digital_literacy_follow_up_sr'->> 'digital_literacy-center-dl'      as digital_literacy_center,
    d->'form'->'enrollment_sr'->'digital_literacy_follow_up_sr'->> 'advanced_it_completion_date_dl'      as advanced_it_completion_date,
    d->'form'->'enrollment_sr'->'digital_literacy_follow_up_sr'->> 'time_of_training_dl'      as time_of_training,
    d->'form'->'enrollment_sr'->'digital_literacy_follow_up_sr'->> 'computer_packages_start_date_dl'      as computer_packages_start_date,
    d->'form'->'enrollment_sr'->'digital_literacy_follow_up_sr'->> 'computer-packages_completion_date_dl'      as computer_packages_completion_date,

    /* --- entrepreneurship_follow_up_questions */
    d->'form'->'enrollment_sr'->'entrepreneurship_follow_up_questions'->> 'start_date_ent'             as entrepreneurship_start_date,
    d->'form'->'enrollment_sr'->'entrepreneurship_follow_up_questions'->> 'completion_date_ent'        as entrepreneurship_completion_date,
    d->'form'->'enrollment_sr'->'entrepreneurship_follow_up_questions'->> 'interest_in_sales_work_ent' as entrepreneurship_interest_in_sales,

    /* --- tvet vocational */
    d->'form'->'enrollment_sr'->'tvetvocational_training_follow_up_questions'->> 'start_date_tvet'         as tvet_start_date,
    d->'form'->'enrollment_sr'->'tvetvocational_training_follow_up_questions'->> 'completion_date_tvet'    as tvet_completion_date,
    d->'form'->'enrollment_sr'->'tvetvocational_training_follow_up_questions'->'placement_details'->> 'course_enrolled_tvet' as tvet_course,
    d->'form'->'enrollment_sr'->'tvetvocational_training_follow_up_questions'->'placement_details'->> 'guardian_alternative_phone_number_tvet' as tvet_guardian_alt_phone,
    d->'form'->'enrollment_sr'->'tvetvocational_training_follow_up_questions'->'placement_details'->> 'name_of_institution_tvet'     as tvet_institution,
    d->'form'->'enrollment_sr'->'tvetvocational_training_follow_up_questions'->'placement_details'->> 'parent_name_tvet'           as tvet_parent_name,
    d->'form'->'enrollment_sr'->'tvetvocational_training_follow_up_questions'->'placement_details'->> 'parent_phone_number_tvet'    as tvet_parent_phone,
    d->'form'->'enrollment_sr'->'tvetvocational_training_follow_up_questions'->'placement_details'->> 'county_placed_tvet'          as tvet_county_placed,

    /* --- sw_ep_handcraft questions */
    d->'form'->'enrollment_sr'->'swep_handcraft_questions_sr'->> 'handcraft_training_type_hc'        as handcraft_training_type,
    d->'form'->'enrollment_sr'->'swep_handcraft_questions_sr'->> 'start_date_hc'                     as handcraft_start_date,
    d->'form'->'enrollment_sr'->'swep_handcraft_questions_sr'->> 'completion_date_hc'               as handcraft_completion_date,
    d->'form'->'enrollment_sr'->'swep_handcraft_questions_sr'->> 'swep_handcrafts_follow_up_questions' as handcraft_follow_up_flag,

    /* --- sw_ep_iga questions */
    d->'form'->'enrollment_sr'->'swep_iga_questions_sr'->> 'training_type_iga'                  as iga_training_type,
    d->'form'->'enrollment_sr'->'swep_iga_questions_sr'->> 'start_date_iga'                     as iga_start_date,
    d->'form'->'enrollment_sr'->'swep_iga_questions_sr'->> 'Completion_date_iga'               as iga_completion_date,
    d->'form'->'enrollment_sr'->'swep_iga_questions_sr'->> 'swep-iga-center-iga'               as iga_center,
    d->'form'->'enrollment_sr'->'swep_iga_questions_sr'->> 'interest_in_sales_work_iga'        as iga_interest_in_sales,

    /* --- apprenticeship */
    d->'form'->'course_of_interest_sr'->'apprenticeship_followup_questions'->'apprenticeship_placement_details'->> 'skill_enrolled_apr'            as apprenticeship_skill,
    d->'form'->'course_of_interest_sr'->'apprenticeship_followup_questions'->'apprenticeship_placement_details'->> 'placement_date_apr'          as apprenticeship_placement_date,
    d->'form'->'course_of_interest_sr'->'apprenticeship_followup_questions'->'apprenticeship_placement_details'->> 'completion_date_apr'         as apprenticeship_completion_date,
    d->'form'->'course_of_interest_sr'->'apprenticeship_followup_questions'->'apprenticeship_placement_details'->> 'guardians_name_apr'           as apprenticeship_guardian_name,
    d->'form'->'course_of_interest_sr'->'apprenticeship_followup_questions'->'apprenticeship_placement_details'->> 'guardians_phone_number_apr'   as apprenticeship_guardian_phone,
    d->'form'->'course_of_interest_sr'->> 'daycare_support_sr'                             as apprenticeship_daycare_support,

    /* --- business mentorship */
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'mentors_details'->> 'region'                     as bm_region,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'mentors_details'->> 'county_bm'                  as bm_county,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'mentors_details'->> 'employees_bm'               as bm_employees,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'mentors_details'->> 'bus_location_bm'            as bm_business_location,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'mentors_details'->> 'name_of_mentor_bm'          as bm_mentor_name,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'mentors_details'->> 'contact_of_mentor_bm'       as bm_mentor_contact,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'mentors_details'->> 'type_of_business_bm'       as type_of_business,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'first_mentorship_visit_bm'->> 'stock_bm'              as bm_stock,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'first_mentorship_visit_bm'->> 'date_first_visit_bm'   as bm_first_visit_date,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'first_mentorship_visit_bm'->> 'cash_to_buy_new_stock_bm' as bm_cash_to_buy_stock,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'first_mentorship_visit_bm'->> 'mentee_business_records_bm' as bm_business_records,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'first_mentorship_visit_bm'->> 'if_mentee_have_dead_stock_bm' as bm_dead_stock,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'first_mentorship_visit_bm'->> 'challenges_mentee_is_facing_bm' as bm_challenges_facing,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'first_mentorship_visit_bm'->> 'solutions_agreed_to_be_implemented_bm' as bm_solutions,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'first_mentorship_visit_bm'->> 'business_costs_before_setting_prices_bm' as bm_business_costs_before_price,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'first_mentorship_visit_bm'->> 'mentees_price_compared_to_competitors_bm' as bm_price_comparison,
    d->'form'->'enrollment_sr'->'business_mentorship_follow_up_question'->'first_mentorship_visit_bm'->> 'agreed_timeline_to_implement_solutions_bm' as bm_solutions_timeline

  from src
)

select * from staged