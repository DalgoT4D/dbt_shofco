{{ config(
  materialized='table',
  tags="education_satisfaction_surveys"
) }}

WITH source_data AS (
    SELECT
        id,
        data AS json_data  -- Cast the data column to JSONB
    FROM {{ source('staging_education', 'Student_Satisfaction_and_Well_Being_Survey') }}
    WHERE data::jsonb->>'archived' IS NULL OR data::jsonb->>'archived' = 'false'
)

SELECT
    -- Student Information
    json_data->'form'->'stxudent_information'->>'student_name' AS student_name,
    json_data->'form'->'student_information'->>'grade' AS grade,
    json_data->'form'->'student_information'->>'term' AS term,
    json_data->'form'->'student_information'->>'school' AS school,
    json_data->'form'->'student_information'->>'your_class_year' AS class_year,

    -- Section A - General Satisfaction
    json_data->'form'->'section_a'->>'section_a_general_satisfaction_with_the_school' AS general_satisfaction,
    json_data->'form'->'section_a'->>'how_satisfied_are_you_with_the_school_in_general_tick_one_box_please' AS school_satisfaction,
    json_data->'form'->'section_a'->>'section_a_commentssuggestions' AS general_satisfaction_comments,

    -- Section B - Teaching Methods
    json_data->'form'->'section_b'->>'section_b_teaching_method_and_teacher_support_to_students' AS teaching_method_satisfaction,
    json_data->'form'->'section_b'->>'how_satisfied_are_you_with_how_the_teachers_teach_in_class_tick_one_box_ple' AS teacher_teaching_satisfaction,
    json_data->'form'->'section_b'->>'how_satisfied_are_you_with_the_way_the_teachers_support_students_to_learn_b' AS teacher_support_satisfaction,
    json_data->'form'->'section_b'->>'section_b1-commentssuggestions' AS teacher_teaching_comments,
    json_data->'form'->'section_b'->>'section_b2_commentssuggestions' AS teacher_support_comments,

    -- Section C - Student-Teacher Relationships
    json_data->'form'->'section_c'->>'section_c_student-teacher_relationship' AS student_teacher_relationship,
    json_data->'form'->'section_c'->>'how_satisfied_are_you_with_how_the_teachers_relate_with_students_and_how_th' AS student_teacher_relationship_satisfaction,
    json_data->'form'->'section_c'->>'section_c-commentssuggestions' AS student_teacher_relationship_comments,

    -- Section D - Library and Reading Materials
    json_data->'form'->'section_d'->>'section_d_access_to_library_and_other_reading_materials' AS library_access,
    json_data->'form'->'section_d'->>'how_satisfied_are_you_with_the_library_services_and_other_reading_materials' AS library_satisfaction,
    json_data->'form'->'section_d'->>'section_d-commentssuggestions' AS library_comments,

    -- Section E - STEM Laboratory and Devices
    json_data->'form'->'section_e'->>'section_e_access_to_steam_laboratory_and_other_learning_devices_like_comput' AS steam_lab_access,
    json_data->'form'->'section_e'->>'how_satisfied_are_you_with_the_steam_laboratory_and_other_learning_devices_' AS steam_lab_satisfaction,
    json_data->'form'->'section_e'->>'section_e-commentssuggestions' AS steam_lab_comments,

    -- Section F - Hygiene and Sanitation
    json_data->'form'->'section_f'->>'section_f_school_hygiene_and_sanitation' AS hygiene_satisfaction,
    json_data->'form'->'section_f'->>'how_satisfied_are_you_with_the_school_hygiene_and_other_sanitation_services' AS sanitation_satisfaction,
    json_data->'form'->'section_f'->>'section_f-commentssuggestions' AS hygiene_comments,

    -- Section G - School Feeding Program
    json_data->'form'->'section_g'->>'section_g_school_feeding_program' AS feeding_program_satisfaction,
    json_data->'form'->'section_g'->>'how_satisfied_are_you_with_the_school_feeding_program_provided_by_the_schoo' AS feeding_satisfaction,
    json_data->'form'->'section_g'->>'section_g_-commentssuggestions' AS feeding_program_comments,

    -- Section H - Extracurricular Activities
    json_data->'form'->'section_h'->>'section_h_extracurricular_activities' AS extracurricular_satisfaction,
    json_data->'form'->'section_h'->>'how_satisfied_are_you_with_the_extracurricular_activities_provided_by_the_s' AS extracurricular_satisfaction_detail,
    json_data->'form'->'section_h'->>'section_h_-commentssuggestions' AS extracurricular_comments,

    -- Section I - Mental Health Support
    json_data->'form'->'section_i'->>'section_i_mental_health_support_provided_by_the_school' AS mental_health_support,
    json_data->'form'->'section_i'->>'how_satisfied_are_you_with_the_mental_health_support_provided_by_the_school' AS mental_health_satisfaction,
    json_data->'form'->'section_i'->>'section_i-commentssuggestions' AS mental_health_comments,

    -- Section J - Likes
    json_data->'form'->'section_j'->>'what_do_you_like_most_about_your_school' AS likes_about_school,

    -- Section K - Improvements
    json_data->'form'->'section_k'->>'section_k_areas_to_improve_at_the_school' AS improvement_areas,
    json_data->'form'->'section_k'->>'if_you_could_change_anything_at_the_school_what_would_it_be' AS proposed_changes,

    -- Section L - Parent Engagement
    json_data->'form'->'section_l'->>'parent_guardian_engagement' AS parent_engagement,

    -- Section M - Well-Being
    json_data->'form'->'section_m'->>'ive_been_feeling_calm' AS feeling_calm,
    json_data->'form'->'section_m'->>'ive_been_in_a_good_mood' AS good_mood,
    json_data->'form'->'section_m'->>'i_always_share_my_sweets' AS share_sweets,
    json_data->'form'->'section_m'->>'ive_been_feeling_relaxed' AS feeling_relaxed

FROM source_data
