SELECT
        (data::json->'form'->>'date_of_visit') AS date_of_visit,
        (data::json->'form'->'satisfaction_with_different_aspect_of_gender_department'->>'being_treated_with_respect')::INTEGER AS being_treated_with_respect,
        (data::json->'form'->'satisfaction_with_different_aspect_of_gender_department'->>'staff_sensitivity_to_your_privacy')::INTEGER AS staff_sensitivity_to_your_privacy,
        (data::json->'form'->'satisfaction_with_different_aspect_of_gender_department'->>'friendliness_and_courtesy_of_staff')::INTEGER AS friendliness_and_courtesy_of_staff,
        (data::json->'form'->'satisfaction_with_different_aspect_of_gender_department'->>'staff_willingness_to_listen_and_offer_assistance')::INTEGER AS staff_willingness_to_listen_and_offer_assistance,
        (data::json->'form'->'satisfaction_with_different_aspect_of_gender_department'->>'staff_conduct_themselves_in_a_professional_manner')::INTEGER AS staff_conduct_themselves_in_a_professional_manner,
        (data::json->'form'->'satisfaction_with_different_aspect_of_gender_department'->>'time_taken_to_offer_the_services_or_support_needed_during_the_case_manageme')::INTEGER AS time_taken_to_offer_services,
        (data::json->'form'->'copy-1-of-on_a_scale_of_1-5_where_1_is_totally_disagree_2_is_disagree__3_is_neither_a'->>'the_service_staff_were_welcoming')::INTEGER AS service_staff_were_welcoming
    FROM {{ source('source_commcare', 'Gender_Client_Satisfaction_Survey') }}
    WHERE data::json->'form'->>'date_of_visit' IS NOT NULL