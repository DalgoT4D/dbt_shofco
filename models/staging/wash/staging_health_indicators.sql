{{ config(
    materialized='table',
    tags=["wash_health_indicators", "wash"]
) }}

    SELECT
        -- Basic metadata
        "data"->>'id' AS "id",
        "data"->'form'->>'date' AS "form_date",
        -- Extract term from form_date
        CASE 
            WHEN "data"->'form'->>'date' ~ '^\d{4}-\d{2}-\d{2}$' THEN
                CASE
                    WHEN EXTRACT(MONTH FROM TO_DATE("data"->'form'->>'date', 'YYYY-MM-DD')) BETWEEN 1 AND 3 THEN 'Term 1'
                    WHEN EXTRACT(MONTH FROM TO_DATE("data"->'form'->>'date', 'YYYY-MM-DD')) BETWEEN 4 AND 7 THEN 'Term 2'
                    WHEN EXTRACT(MONTH FROM TO_DATE("data"->'form'->>'date', 'YYYY-MM-DD')) BETWEEN 8 AND 12 THEN 'Term 3'
                END
            ELSE NULL
        END AS "term",
        "data"->'form'->>'school_name' AS "school_name",
        "data"->'form'->'school_population'->>'number_of_health_club_members_boys' AS "health_club_boys",
        "data"->'form'->'school_population'->>'number_of_health_club_members_girls' AS "health_club_girls",

        -- Health club status
        CASE
            WHEN "data"->'form'->'monitoring_indicator_2'->>'an_active_and_representative_health_club_established_check_the_record_of_th' = 'yes'
            THEN 1 ELSE 0
        END AS "health_club_active",

        (
            CASE WHEN "data"->'form'->'monitoring_indicator_1'->>'weekly_health_club_meetings_conducted_check_the_club_register_and_minutes_o' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_2'->>'an_active_and_representative_health_club_established_check_the_record_of_th' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_3'->>'the_health_club_members_have_been_taken_through_water_sanitation_and_hygien' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_4'->>'observe_the_cleanliness_of_pupilsstudents_check_the_school_uniformsphysical' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_5'->>'observe_if_the_school_has_a_water_storage_reservoir_check_the_capacity_and_' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_6'->>'all_pupils_drink_safe_water_observe_the_source_of_drinking_water_and_inquir' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_7'->>'structural_aspect_of_the_toilet' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_8'->>'toiletslatrines_are_used_and_maintained_well_observation_of_the_cleanliness' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_9'->>'observe_the_sanitation_ratio_student-toilet_ration_as_per_the_public_health' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_10'->>'cleaning_roster_confirm_if_it_is_daily' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_11'->>'school_environment_is_kept_clean_observe_the_cleanliness_of_the_classrooms_' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_12'->>'school_classroom_is_kept_clean_observe_the_cleanliness_of_the_classrooms_an' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_13'->>'refuse_well_managed_and_refuse_pit_kept_clean_ask_if_the_refuse_is_managed_' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_14'->>'food_is_prepared_hygienically_observe_the_hygiene_of_the_food_preparation_a' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_15'->>'observe_food_containers_discourage_serving_food_in_plastics' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_16'->>'observe_if_the_school_has_functional_and_adequate_hand-washing_facilities_c' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_17'->>'observe_handwashing_practice_with_running_water_and_soap_observe_children_d' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_18'->>'interview_a_few_pupils_if_they_receive_sanitary_towels_sample_pupils_from_d' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_19'->>'observe_how_used_sanitary_towels_are_disposed_accordingly_check_the_availab' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_20'->>'inquire_from_the_club_members_if_they_take_lead_in_planning_for_hygiene_pro' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_21'->>'observe_whether_the_soaps_are_strategically_placed_at_the_hand_washing_stat' = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN "data"->'form'->'monitoring_indicator_22'->>'observe_if_the_school_has_water_for_hand_washing_check_the_presence_of_wate' = 'yes' THEN 1 ELSE 0 END
        ) AS "hygiene_score"
    FROM {{ source('staging_wash', 'SCHOOL_HEALTH_CLUB_MONITORING_INDICATORS') }}
