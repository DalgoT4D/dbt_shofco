{{ config(
    materialized='table',
    tags=['sl', 'placement_followup']
) }}

WITH raw_data AS (
    SELECT
        data::jsonb AS form_data,
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS form_id,
        data::jsonb -> 'form' -> 'meta' ->> 'username' AS enumerator_username,
        data::jsonb -> 'form' -> 'meta' ->> 'userID' AS enumerator_user_id,
        data::jsonb ->> 'received_on' AS submission_date,

        -- Case data
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,
        data::jsonb -> 'form' -> 'case' ->> '@date_modified' AS date_modified,

        -- Participant info
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_fullname' AS participant_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_sex' AS sex,

        CASE
            WHEN NULLIF(data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age', '') IS NOT NULL
                 AND (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age') ~ '^[0-9]*\.?[0-9]+$'
            THEN FLOOR((data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age')::FLOAT)::INT
            ELSE NULL
        END AS age,

        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_county' AS county,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_subcounty' AS subcounty,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_ahofco_ward' AS ward,

        -- Employment placement info
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'job_position_pl' AS job_position,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'income_on_average_pl' AS average_income,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'name_of_organization_pl' AS organization_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'organization_sector_pl' AS organization_sector,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'nature_of_emplyment_pl' AS nature_of_employment,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'placement_opportunity_pl' AS placement_opportunity,

        -- Skills/training data
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'training_activity_pl' AS training_activity,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'skills_gained_pl' AS skills_gained,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'certifications_achieved_pl' AS certifications_achieved,

        -- Transition metadata
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'region_of_business_transition_pl' AS region_of_transition,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'county_of_job_transition_pl' AS county_of_transition,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'participant_program_pl' AS participant_program,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'new_role_through_shofco_support_pl' AS new_role_through_shofco_support,

        -- Extra metadata
        data::jsonb -> 'metadata' ->> 'submit_ip' AS submit_ip

    FROM {{ source('staging_sl', 'Placement_Followup') }}
    WHERE (data::jsonb ->> 'archived') IS NULL OR (data::jsonb ->> 'archived') = 'false'
)

SELECT *
FROM raw_data
