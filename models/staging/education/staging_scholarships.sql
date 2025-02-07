{{ config(
  materialized='table',
  tags=["education_expansion", "education"]
) }}

SELECT
    "Term" AS term,
    "Year" AS year,
    "Cohort" AS cohort,
    "Orphan_" AS orphan,
    "Teen_Mom_" AS teen_mom,
    "Home_County" AS county,
    "GBV_survivor_" AS gbv_survivor,
    "Current_Status" AS current_status,
    "Form__current_" AS form,
    "Special_needs_" AS special_needs,
    "Boarding_OR_Day" AS boarding_or_day,
    "Home_Sub_County" AS subcounty,
    "School_Location" AS school_location,
    "Public_OR_Private" AS public_or_private,
    "Form__when_awarded_" AS form_when_awarded,
    "Official_School_Name" AS school_name,
    "Became_pregnant_during_program" AS became_pregnant_during_program,
    "Experienced_GBV_during_program" AS experienced_gbv_during_program,
    "Scholarship_Enrollment_Status_T1_2025" AS scholarship_enrollment_status_t1_2025,
    "Scholarship_Enrollment_Status_T2_2025" AS scholarship_enrollment_status_t2_2025,
    "Scholarship_Enrollment_Status_T3_2024" AS scholarship_enrollment_status_t3_2024,
    "Scholarship_Enrollment_Status_T3_2025" AS scholarship_enrollment_status_t3_2025,
    "Student_Name__As_Captured_on_Application_" AS school_name_on_application,
    "Referred_to_gender_for_psychosocial_counselling_" AS referred_to_psychosocial_counselling,
    _airbyte_extracted_at
FROM {{ source('staging_education', 'Scholarships') }}
