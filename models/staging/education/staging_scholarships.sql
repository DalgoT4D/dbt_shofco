{{ config(
  materialized='table',
  tags=["education_expansion", "education"]
) }}

SELECT
  "Term" as "term",
  "Year" as "year",
  "Cohort" as "cohort",	
  "Orphan_" as "orphan",
  "Teen_Mom_" as "teen_mom",	
  "Home_County" as "county",
  "GBV_survivor_" as "gbv_survivor",	
  "Current_Status" as "current_status",
  "Form__current_" as "form",
  "Special_needs_" as "special_needs",
  "Boarding_OR_Day" as "boarding_or_day",
  "Home_Sub_County" as "subcounty",
  "School_Location" as "school_location",
  "Public_OR_Private" as "public_or_private",	
  "Form__when_awarded_" as "form_when_awarded",
  "Official_School_Name" as "school_name",
  "Became_pregnant_during_program" as "became_pregnant_during_program",
  "Experienced_GBV_during_program" as "experienced_gbv_during_program",
  "Scholarship_Enrollment_Status_T1_2025" as "scholarship_enrollment_status_t1_2025",
  "Scholarship_Enrollment_Status_T2_2025" as "scholarship_enrollment_status_t2_2025",
  "Scholarship_Enrollment_Status_T3_2024" as "scholarship_enrollment_status_t3_2024",
  "Scholarship_Enrollment_Status_T3_2025" as "scholarship_enrollment_status_t3_2025",
  "Student_Name__As_Captured_on_Application_" as "school_name_on_application",
  "Referred_to_gender_for_psychosocial_counselling_" as "referred_to_psychosocial_counselling",
  {{ validate_date("_airbyte_extracted_at") }} AS _airbyte_extracted_at
FROM {{ source('staging_education', 'Scholarships') }}
