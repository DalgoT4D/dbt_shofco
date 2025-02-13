{{ config(
  materialized='table',
  tags=["education_expansion", "education"]
) }}

SELECT
  "Year" as "year",
  "School_Name" as "school_name",
  "School_County" as "county",	
  "Mean_KPCE_Score" as "mean_kpce_score",
  "School_Subcounty" as "subcounty",	
  "__Teachers_trained" as "teachers_trained",	
  "__Students_enrolled" as "students_enrolled",
  "__Computers_provided" as "computers_provided",	
  "T1_Average_Attendance" as "t1_average_attendance",
  "T2_Average_Attendance" as "t2_average_attendance",	
  "T3_Average_Attendance" as "t3_average_attendance",
  "__Toilet_stances_built" as "toilet_stances_built",	
  "High_Touch___Low_Touch" as "high_touch_low_touch",
  "Revised_Toilet_Student_Ratio" as "revised_toilet_student_ratio",
  "Baseline_Toilet_Student_Ratio" as "baseline_toilet_student_ratio",
  "Teacher_Training_Satisfaction" as "teacher_training_satisfaction",
  "_airbyte_extracted_at" AS _airbyte_extracted_at
FROM {{ source('staging_education', 'Public_School_Partnerships') }}
