{{ config(
  materialized='table',
  tags=["education_satisfaction_surveys", "education"]
) }}

WITH source_data AS (
    SELECT
        id,
        data AS json_data
    FROM {{ source('staging_education', 'Teachers_Satisfaction_Survey') }}
    WHERE
        data::jsonb ->> 'archived' IS NULL
        OR data::jsonb ->> 'archived' = 'false'
)

SELECT
    -- Basic Information
    json_data -> 'form' ->> 'name_of_school' AS school_name,
    json_data
    -> 'form'
    ->> 'which_level_do_you_teach_please_tick_one_of_the_following' AS teaching_level,

    CASE
        WHEN json_data ->> 'received_on' IS NOT NULL
            THEN
                EXTRACT(
                    YEAR FROM TO_TIMESTAMP(
                        LEFT(json_data ->> 'received_on', 19),
                        'YYYY-MM-DD"T"HH24:MI:SS'
                    )
                )
    END AS year,
    -- Section A - General Satisfaction
    json_data
    -> 'form'
    -> 'section_a'
    ->> 'section_a_general_satisfaction_with_the_shofco_school' AS general_satisfaction,
    json_data
    -> 'form'
    -> 'section_a'
    ->> 'how_satisfied_are_you_with_the_education_received_by_your_child_at_the_scho' AS education_satisfaction,

    -- Section B - Parent-Teacher Relationships
    json_data
    -> 'form'
    -> 'section_b'
    ->> 'section_b_parents_and_teachers_relationships' AS parent_teacher_relationship,
    json_data
    -> 'form'
    -> 'section_b'
    ->> 'how_satisfied_are_you_with_how_parents_and_teachers_relate_at_the_school_on' AS relationship_satisfaction,

    -- Section C - Student-Teacher Ratio
    json_data
    -> 'form'
    -> 'section_c'
    ->> 'section_c_student-teacher_ratio' AS student_teacher_ratio,
    json_data
    -> 'form'
    -> 'section_c'
    ->> 'how_satisfied_are_you_with_the_number_of_students_handled_by_each_teacher_b' AS student_teacher_ratio_satisfaction,
    json_data
    -> 'form'
    -> 'section_c'
    ->> 'sectionc_commentsrecommendation' AS student_teacher_ratio_comments,

    -- Section D - Hygiene and Sanitation
    json_data
    -> 'form'
    -> 'section_d'
    ->> 'section_d_school_hygiene_and_sanitation' AS hygiene_satisfaction,
    json_data
    -> 'form'
    -> 'section_d'
    ->> 'how_satisfied_are_you_with_the_school_hygiene_and_other_sanitation_services' AS sanitation_satisfaction,
    json_data
    -> 'form'
    -> 'section_d'
    ->> 'sectiond_commentssuggestions' AS sanitation_comments,

    -- Section E - School Feeding Program
    json_data
    -> 'form'
    -> 'section_e'
    ->> 'section_e_school_feeding_program' AS feeding_program_satisfaction,
    json_data
    -> 'form'
    -> 'section_e'
    ->> 'how_satisfied_are_you_with_the_school_feeding_program_provided_by_the_schoo' AS feeding_satisfaction,
    json_data
    -> 'form'
    -> 'section_e'
    ->> 'sectione_commentssuggestions' AS feeding_program_comments,

    -- Section F - Extracurricular Activities
    json_data
    -> 'form'
    -> 'section_f'
    ->> 'section_f_extracurricular_activities' AS extracurricular_satisfaction,
    json_data
    -> 'form'
    -> 'section_f'
    ->> 'how_satisfied_are_you_with_the_extracurricular_activities_provided_by_the_s' AS extracurricular_satisfaction_detail,
    json_data
    -> 'form'
    -> 'section_f'
    ->> 'sectionf_commentssuggestions' AS extracurricular_comments,

    -- Section G - Teacher Engagement
    json_data
    -> 'form'
    -> 'section_g'
    ->> 'section_g_teacher_engagement_beyond_normal_class_teaching' AS teacher_engagement,
    json_data
    -> 'form'
    -> 'section_g'
    ->> 'how_satisfied_are_you_with_the_level_of_teacher_engagement_in_non-teaching_' AS teacher_engagement_satisfaction,
    json_data
    -> 'form'
    -> 'section_g'
    ->> 'sectiong_commentssuggestions' AS teacher_engagement_comments,

    -- Section H - Suggestions for Improvement
    json_data
    -> 'form'
    -> 'section_h'
    ->> 'section_h_suggestions_for_improvement_at_the_school' AS improvement_suggestions,
    json_data
    -> 'form'
    -> 'section_h'
    ->> 'if_you_could_propose_some_changes_at_the_school_what_could_those_be' AS proposed_changes,

    -- Additional Details
    json_data
    -> 'form'
    ->> 'background_and_purpose_in_order_to_continuously_improve_on_our_education_se' AS survey_background

FROM source_data
