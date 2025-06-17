{{ config(
  materialized='table',
  tags=['placement_followup', 'sl']
) }}

WITH placement_data AS (
    SELECT
        id,
        -- Case metadata
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator' AS enumerator,

        -- Participant details
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_fullname' AS fullname,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_unique_id' AS participant_id,

        -- Age with validation and casting
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

        -- Placement & program fields
        data::jsonb -> 'form' ->> 'training_activity_pl' AS training_activity,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'placement_opportunity_pl' AS placement_opportunity,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'type_of_business_pl' AS type_of_business,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'participant_program_pl' AS participant_program,
        data::jsonb -> 'form' ->> 'certifications_achieved_pl' AS certifications_achieved,
        data::jsonb -> 'form' ->> 'skills_gained_pl' AS skills_gained,

        -- Raw average income value
        data::jsonb -> 'form' -> 'business_transitions' ->> 'income_on_average_pl' AS income_raw,

        -- Additional fields for analytics
        data::jsonb -> 'form' -> 'business_transitions' ->> 'new_role_through_shofco_support_pl' AS new_role_source_pl,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'documents_pl' AS documents_held_pl,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'organization_sector_pl' AS sector_pl,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'nature_of_emplyment_pl' AS employment_type_pl,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'date_started_business_pl' AS business_start_date_pl,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'average_employees_pl' AS avg_employees_pl,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'average_female_employees_pl' AS avg_female_employees_pl,

        -- County and region of placement
        data::jsonb -> 'form' -> 'business_transitions' ->> 'county_of_job_transition_pl' AS county_of_transition,
        data::jsonb -> 'form' -> 'business_transitions' ->> 'region_of_business_transition_pl' AS region_of_transition,

        -- Submission details
        data::jsonb ->> 'received_on' AS date_received

    FROM {{ source('staging_sl', 'Placement_Followup') }}
    WHERE data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
)

SELECT DISTINCT
    id,
    case_id,
    user_id,
    enumerator,
    fullname,
    participant_id,
    age,
    sex,
    county,
    subcounty,
    ward,
    training_activity,
    placement_opportunity,
    type_of_business,
    participant_program,
    certifications_achieved,
    skills_gained,
    county_of_transition,
    region_of_transition,
    date_received,
    new_role_source_pl,
    documents_held_pl,
    sector_pl,
    employment_type_pl,

    -- Final cleaned average income in KES
    CASE
        WHEN income_raw ILIKE 'below_kshs_5000' THEN '<5000'
        WHEN income_raw ~* '^kshs?_' THEN REGEXP_REPLACE(income_raw, '^kshs?_?', '', 'i')
        WHEN income_raw ILIKE 'EMPTY' THEN NULL
        ELSE income_raw
    END AS average_income_in_kes

FROM placement_data
