{{ config(
    materialized='table',
    tags=['sl', 'placement_followup']
) }}

WITH raw_data AS (
    SELECT
        data::jsonb AS form_data,

        -- Metadata
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS form_id,
        data::jsonb -> 'form' -> 'meta' ->> 'username' AS enumerator_username,
        data::jsonb -> 'form' -> 'meta' ->> 'userID' AS enumerator_user_id,
        data::jsonb ->> 'received_on' AS submission_date,

        -- Case data
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,
        data::jsonb -> 'form' -> 'case' ->> '@date_modified' AS date_modified,

        -- Participant info from case.update
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_fullname' AS participant_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_unique_id' AS unique_id,

        CASE
            WHEN NULLIF(data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age', '') IS NOT NULL
                 AND (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age') ~ '^[0-9]*\.?[0-9]+$'
            THEN FLOOR((data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age')::FLOAT)::INT
            ELSE NULL
        END AS age,

        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_sex' AS sex,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_county' AS county,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_subcounty' AS subcounty,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_ahofco_ward' AS ward,

        -- Employment placement info (from nested form.business_transitions)
        data::jsonb -> 'form' -> 'business_transitions' ->> 'type_of_business_pl' AS type_of_business,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'income_on_average_pl' AS average_income,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'documents_pl' AS documents,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'placement_opportunity_pl' AS placement_opportunity,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'county_of_job_transition_pl' AS county_of_transition,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'region_of_business_transition_pl' AS region_of_transition,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'participant_program_pl' AS participant_program,

        -- Training and skills (from form root)
        data::jsonb -> 'form' ->> 'training_activity_pl' AS training_activity,
        data::jsonb -> 'form' ->> 'certifications_achieved_pl' AS certifications_achieved,
        data::jsonb -> 'form' ->> 'skills_gained_pl' AS skills_gained,

        -- Staff and extras
        data::jsonb -> 'form' ->> 'staff_name_pl' AS staff_name,

        -- Submit IP (from root)
        data::jsonb ->> 'submit_ip' AS submit_ip

    FROM {{ source('staging_sl', 'Placement_Followup') }}
    WHERE (data::jsonb ->> 'archived') IS NULL OR (data::jsonb ->> 'archived') = 'false'
)

SELECT *
FROM raw_data
