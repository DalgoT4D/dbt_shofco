{{ config(
  materialized='table',
    tags=["education_well_being_sessions", "education"]
) }}

SELECT  
    {{ validate_date("Date") }} AS "date",           
    "Grade" as "grade",          
    "Topic" as "topic",               
    LOWER("School") as "school_type",             
    "Stream" as "stream",              
    CAST("Number_of_stdents_trained" AS INTEGER) AS "number_of_students_trained", 
    "Session_Type" as "session_type"    
FROM {{ ref("staging_well_being_sessions") }}