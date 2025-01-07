{{ config(
  materialized='table',
  tags="education_expansion"
) }}

SELECT
  "Year",
  "School_Name",
  "School_County",	
  "Mean_KPCE_Score",
  "School_Subcounty",	
  "__Teachers_trained",	
  "__Students_enrolled",
  "__Computers_provided",	
  "T1_Average_Attendance",
  "T2_Average_Attendance",	
  "T3_Average_Attendance",
  "__Toilet_stances_built",	
  "High_Touch___Low_Touch",
  "Revised_Toilet_Student_Ratio",
  "Baseline_Toilet_Student_Ratio",
  "Teacher_Training_Satisfaction",
  "_airbyte_raw_id",
  "_airbyte_extracted_at",
  "_airbyte_meta"
FROM {{ source('staging_education', 'Public_School_Partnerships') }}