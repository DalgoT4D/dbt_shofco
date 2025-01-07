{{ config(
  materialized='table',
  tags="education_attendance"
) }}

SELECT
  "Term",
  "Year",
  "Grade",
  "Cohort",	
  "Meeting_Date",
  "Meeting_Type",
  "Number_Present",	
  "Number_of_Parents",	
  "Attendance_Percentage_",	
  "Share_of_Parents_Engaged",	
  "Attendance_Data_Collected",
  "_airbyte_raw_id",
  "_airbyte_extracted_at",
  "_airbyte_meta",
  'KSG' AS "school_type"
FROM {{ source('staging_education', 'KSG_Parents_Summary') }}
UNION ALL
SELECT
  "Term",
  "Year",
  "Grade",
  "Cohort",	
  "Meeting_Date",
  "Meeting_Type",
  "Number_Present",	
  "Number_of_Parents",	
  "Attendance_Percentage_",	
  "Share_of_Parents_Engaged",	
  "Attendance_Data_Collected",
  "_airbyte_raw_id",
  "_airbyte_extracted_at",
  "_airbyte_meta",
  'MSG' AS "school_type"
FROM {{ source('staging_education', 'MSG_Parents_Summary') }}