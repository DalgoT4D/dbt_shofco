{{ config(
  materialized='table',
  tags="education_satisfaction_surveys"
) }}

WITH source_data AS (
    SELECT
        id,
        data AS json_data
    FROM {{ source('staging_education', 'Parent_Satisfaction_Survey') }}
)
SELECT
    -- Basic Details
    json_data->'form'->>'student_name' AS student_name,
    json_data->'form'->>'grade' AS grade,
    json_data->'form'->>'term' AS term,
    json_data->'form'->>'school' AS school,
    json_data->'form'->>'age_of_your_child' AS child_age,
    json_data->'form'->>'class_year_of_child' AS class_year_of_child,
    json_data->'form'->>'parentguardian_name' AS parent_name,
    
    -- Satisfaction Scores
    json_data->'form'->'section_a'->>'section_a_general_satisfaction_with_the_shofco_school' AS general_satisfaction,
    json_data->'form'->'section_a'->>'how_satisfied_are_you_with_the_education_received_by_your_child_at_the_scho' AS education_satisfaction,
    json_data->'form'->'section_b'->>'section_b_parents_and_teachers_relationships' AS teacher_parent_relationship,
    json_data->'form'->'section_b'->>'how_satisfied_are_you_with_how_parents_and_teachers_relate_at_the_school_on' AS relationship_satisfaction,
    json_data->'form'->'section_c'->>'section_c_student-teacher_ratio' AS student_teacher_ratio,
    json_data->'form'->'section_c'->>'how_satisfied_are_you_with_the_number_of_students_handled_by_each_teacher_b' AS student_teacher_ratio_satisfaction,
    json_data->'form'->'section_d'->>'section_d_school_hygiene_and_sanitation' AS hygiene_satisfaction,
    json_data->'form'->'section_d'->>'how_satisfied_are_you_with_the_school_hygiene_and_other_sanitation_services' AS sanitation_satisfaction,
    json_data->'form'->'section_e'->>'section_e_school_feeding_program' AS feeding_program_satisfaction,
    json_data->'form'->'section_e'->>'how_satisfied_are_you_with_the_school_feeding_program_provided_by_the_schoo' AS feeding_satisfaction,
    json_data->'form'->'section_f'->>'section_f_extracurricular_activities' AS extracurricular_satisfaction,
    json_data->'form'->'section_f'->>'how_satisfied_are_you_with_the_extracurricular_activities_provided_by_the_s' AS extracurricular_satisfaction_detail,
    json_data->'form'->'section_g'->>'section_g_parental_engagement_requested_by_the_school' AS parental_engagement_satisfaction,
    json_data->'form'->'section_g'->>'how_satisfied_are_you_with_the_level_of_parental_engagement_requested_by_th' AS parental_engagement_satisfaction_detail,
    
    -- Improvement Suggestions
    json_data->'form'->'section_i'->>'section_i_suggestions_for_improvement_at_the_school' AS improvement_suggestions,
    json_data->'form'->'section_i'->>'if_you_could_propose_some_changes_at_the_school_what_could_those_be' AS proposed_changes,
    
    -- Changes Observed (Section H)
    json_data->'form'->'section_h'->>'since_your_daughter_joined_the_shofco_school_which_changes_have_you_seen_in' AS observed_changes,
    json_data->'form'->'section_h'->>'section_h_changes_you_have_seen_in_your_child' AS changes_in_child,

    -- Other Details
    json_data->'form'->>'in_the_past_one_year_has_you_or_any_member_of_your_family_benefitted_from_o' AS family_benefit,
    json_data->'form'->>'parents_satisfaction_survey_background_and_purpose_in_order_to_continuously' AS survey_background,
    
    -- Submission Details
    json_data->>'received_on' AS submission_time

FROM source_data