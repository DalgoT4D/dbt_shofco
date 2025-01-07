{{ config(
  materialized='table',
  tags="education_expansion"
) }}

SELECT
  "Term",
  "Year",
  "Cohort",	
  "Orphan_",
  "Teen_Mom_",	
  "Home_County",
  "GBV_survivor_",	
  "Current_Status",
  "Form__current_",
  "Special_needs_",
  "Boarding_OR_Day",
  "Home_Sub_County",
  "School_Location",
  "Public_OR_Private",	
  "Form__when_awarded_",
  "Official_School_Name",
  "Became_pregnant_during_program",
  "Experienced_GBV_during_program",
  "Scholarship_Enrollment_Status_T1_2025",
  "Scholarship_Enrollment_Status_T2_2025",
  "Scholarship_Enrollment_Status_T3_2024",
  "Scholarship_Enrollment_Status_T3_2025",
  "Student_Name__As_Captured_on_Application_",
  "Referred_to_gender_for_psychosocial_counselling_",
  "_airbyte_raw_id",
  "_airbyte_extracted_at",
  "_airbyte_meta"
FROM {{ source('staging_education', 'Scholarships') }}