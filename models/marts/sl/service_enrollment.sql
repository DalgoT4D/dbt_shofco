{{ config(
  materialized='table',
  tags=["service_enrollment", "sl"]
) }}

WITH service_data AS (
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(pp_fullname), '\s+', ' ', 'g')) AS norm_pp_name,
        LOWER(TRIM(enumerator_full_name)) AS norm_enumerator,
        CASE 
            WHEN TRIM(pp_age) ~ '^\d+$' THEN CAST(pp_age AS INTEGER)
            ELSE NULL
        END AS clean_age
    FROM {{ ref('staging_service_enrollment') }}
),

register_data AS (
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(full_name), '\s+', ' ', 'g')) AS norm_reg_name,
        LOWER(TRIM(enumerator_full_name)) AS norm_enumerator
    FROM {{ ref('staging_register') }}
),

matched_data AS (
    SELECT DISTINCT ON (sd.case_id)
        sd.*,
        rd.county AS reg_county,
        rd.subcounty AS reg_subcounty,
        rd.ward AS reg_ward,
        rd.age AS reg_age,
        rd.full_name AS matched_name,
        rd.enumerator_full_name AS matched_enumerator,
        rd.id AS matched_register_id,

        CASE 
            WHEN sd.norm_pp_name = rd.norm_reg_name AND sd.norm_enumerator = rd.norm_enumerator AND sd.clean_age = rd.age THEN 'strict'
            WHEN sd.norm_pp_name = rd.norm_reg_name AND sd.norm_enumerator = rd.norm_enumerator THEN 'medium'
            WHEN sd.norm_pp_name = rd.norm_reg_name THEN 'loose'
            ELSE 'none'
        END AS match_level

    FROM service_data sd
    LEFT JOIN register_data rd
        ON (
            sd.norm_pp_name = rd.norm_reg_name
            AND sd.norm_enumerator = rd.norm_enumerator
            AND sd.clean_age = rd.age
        )
        OR (
            sd.norm_pp_name = rd.norm_reg_name
            AND sd.norm_enumerator = rd.norm_enumerator
        )
        OR (
            sd.norm_pp_name = rd.norm_reg_name
        )
)

SELECT
  case_id,
  pp_fullname AS full_name,
  pp_sex AS gender,
  clean_age AS age,
  pp_unique_id AS unique_id,
  course_enrolled_sr AS enrolled_course,
  ongoing_course,

  -- ✅ Proper fallback logic for locations
  CASE
    WHEN NULLIF(TRIM(pp_shofco_county), '') IS NOT NULL THEN pp_shofco_county
    WHEN NULLIF(TRIM(reg_county), '') IS NOT NULL THEN reg_county
    ELSE NULL
  END AS county,

  CASE
    WHEN NULLIF(TRIM(pp_shofco_subcounty), '') IS NOT NULL THEN pp_shofco_subcounty
    WHEN NULLIF(TRIM(reg_subcounty), '') IS NOT NULL THEN reg_subcounty
    ELSE NULL
  END AS subcounty,

  CASE
    WHEN NULLIF(TRIM(pp_ahofco_ward), '') IS NOT NULL THEN pp_ahofco_ward
    WHEN NULLIF(TRIM(reg_ward), '') IS NOT NULL THEN reg_ward
    ELSE NULL
  END AS ward,

  -- Debug info
  match_level,
  matched_name,
  matched_enumerator,
  reg_age,

  participants_program_sr AS program,
  is_existing_participant,
  previous_service_completed,
  skills_before_joining_shofco_sr,

  user_location_id AS location_id,
  enumerators_first AS enumerator_firstname,
  enumerator_last_name AS enumeratorlastname,
  enumerator_full_name AS enumerator,

  -- daycare
  daycare_children_under_4,
  daycare_number_of_children,
  daycare_do_you_have_children,
  daycare_children_with_disabilities,
  daycare_details,

  -- dignity kit
  dignity_kit_support,
  dignity_experienced_days_nothing,
  dignity_miss_school,
  dignity_challenges_hygiene,

  -- transitions
  transition_employment_type,
  transition_preferred_region,
  transition_computer_literate,
  transition_willingness_to_relocate,
  transition_job_placement_support_needed,
  transition_post_training_support_needed,
  transition_preferred_sectors,
  transition_support_entrepreneur,

  -- type of work
  participants_type_of_work_sr,
  employees_sr,
  type_of_business_sr,
  business_location_sr,
  year_business_started_sr,
  monthly_business_income_sr,

  -- internship
  internship_position,
  internship_org,
  internship_supervisor,
  internship_supervisor_phone,
  {{ validate_date('internship_start_date') }} AS internship_start_date,
  {{ validate_date('internship_completion_date') }} AS internship_completion_date,

  -- tailoring
  tailoring_center,
  {{ validate_date('tailoring_start_date') }} AS tailoring_start_date,
  {{ validate_date('tailoring_expected_end') }} AS tailoring_expected_end,

  -- digital literacy
  digital_literacy_course,
  {{ validate_date('digital_literacy_start_date') }} AS digital_literacy_start_date,
  {{ validate_date('digital_literacy_completion_date') }} AS digital_literacy_completion_date,
  digital_literacy_training_time,

  -- entrepreneurship
  {{ validate_date('entrepreneurship_start_date') }} AS entrepreneurship_start_date,
  {{ validate_date('entrepreneurship_completion_date') }} AS entrepreneurship_completion_date,
  entrepreneurship_interest_in_sales,

  -- tvet
  {{ validate_date('tvet_start_date') }} AS tvet_start_date,
  {{ validate_date('tvet_completion_date') }} AS tvet_completion_date,
  tvet_course,
  tvet_guardian_alt_phone,
  tvet_institution,
  tvet_parent_name,
  tvet_parent_phone,
  tvet_county_placed,

  -- handcraft
  handcraft_training_type,
  {{ validate_date('handcraft_start_date') }} AS handcraft_start_date,
  {{ validate_date('handcraft_completion_date') }} AS handcraft_completion_date,
  handcraft_follow_up_flag,

  -- iga
  iga_training_type,
  {{ validate_date('iga_start_date') }} AS iga_start_date,
  {{ validate_date('iga_completion_date') }} AS iga_completion_date,
  iga_center,
  iga_interest_in_sales,

  -- apprenticeship
  apprenticeship_skill,
  {{ validate_date('apprenticeship_placement_date') }} AS apprenticeship_placement_date,
  {{ validate_date('apprenticeship_completion_date') }} AS apprenticeship_completion_date,
  apprenticeship_guardian_name,
  apprenticeship_guardian_phone,
  apprenticeship_daycare_support,

  -- business mentorship
  bm_region,
  bm_county,
  bm_employees,
  bm_business_location,
  bm_mentor_name,
  bm_mentor_contact,
  type_of_business,
  bm_stock,
  {{ validate_date('bm_first_visit_date') }} AS bm_first_visit_date,
  bm_cash_to_buy_stock,
  bm_business_records,
  bm_dead_stock,
  bm_challenges_facing,
  bm_solutions,
  bm_business_costs_before_price,
  bm_price_comparison,
  bm_solutions_timeline

FROM matched_data
