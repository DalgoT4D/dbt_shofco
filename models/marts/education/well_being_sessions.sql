{{ config(
  materialized='table'
) }}

SELECT  
    TO_DATE("Date", 'DD/MM/YYYY') AS "Date",           
    "Grade" as "grade",          
    "Topic",               
    LOWER("School") as "school_type",             
    "Stream",              
    CAST("Number_of_stdents_trained" AS INTEGER) AS "Number_of_stdents_trained", -- Cast to INTEGER
    "Session_Type"    
FROM {{ ref("staging_well_being_sessions") }}