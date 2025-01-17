{{ config(
  materialized='table',
  tags="education_expansion"
) }}

SELECT
    "Year" AS year,
    "School_Name" AS school_name,
    "School_County" AS county,	
    "Mean_KPCE_Score" AS mean_kpce_score,
    "School_Subcounty" AS subcounty,	
    "__Teachers_trained" AS teachers_trained,	
    "__Students_enrolled" AS students_enrolled,
    "__Computers_provided" AS computers_provided,	
    "T1_Average_Attendance" AS t1_average_attendance,
    "T2_Average_Attendance" AS t2_average_attendance,	
    "T3_Average_Attendance" AS t3_average_attendance,
    "__Toilet_stances_built" AS toilet_stances_built,	
    "High_Touch___Low_Touch" AS high_touch_low_touch,
    "Revised_Toilet_Student_Ratio" AS revised_toilet_student_ratio,
    "Baseline_Toilet_Student_Ratio" AS baseline_toilet_student_ratio,
    "Teacher_Training_Satisfaction" AS teacher_training_satisfaction,
    _airbyte_extracted_at
FROM {{ source('staging_education', 'Public_School_Partnerships') }}
