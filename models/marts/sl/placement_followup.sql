{{ config(
  materialized='table',
  tags=['placement_followup', 'sl', "sl_marts"]
) }}

SELECT
    id,
    case_id,
    user_id,
    enumerator,
    pp_fullname AS fullname,
    pp_unique_id AS participant_id,
    CASE WHEN age ~ '^[0-9]+$' THEN age::INT ELSE NULL END AS age,  -- Using coalesced field
    sex AS sex,  -- Using coalesced field
    county AS county,
    pp_shofco_subcounty AS subcounty,
    pp_ahofco_ward AS ward,
    training_activity_pl AS training_activity,
    placement_opportunity_pl AS placement_opportunity,
    type_of_business_pl AS type_of_business,
    participant_program_pl AS participant_program,
    certifications_achieved_pl AS certifications_achieved,
    skills_gained_pl AS skills_gained,
    county_of_job_transition_pl AS county_of_transition,
    region_of_business_transition_pl AS region_of_transition,
    income_on_average_pl AS average_income_in_kes,
    indexed_on AS date_received,

    -- Renamed fields for clarity
    COALESCE(new_role_through_shofco_support_pl, new_role_through_individual_support_pl) AS new_role_source,
    documents AS documents_held,
    organization_sector_pl AS organization_sector,
    nature_of_emplyment_pl AS nature_of_employment,

    -- Business and employee-related fields
    NULL AS business_start_date_pl,
    CASE 
        WHEN year_business_started_pl ~ '^\d{4}$' THEN year_business_started_pl::INT
        ELSE NULL
    END AS year_business_started,
    CASE WHEN employees_pl = 'yes' THEN true ELSE false END AS has_employees_pl,
    number_of_employees_pl,
    female_employees_pl,
    number_of_employees_pl AS avg_employees_pl,
    female_employees_pl AS avg_female_employees_pl,

    -- Business age calculation (in years, from year_business_started)
    CASE 
        WHEN year_business_started_pl ~ '^\d{4}$' 
        THEN EXTRACT(YEAR FROM CURRENT_DATE) - year_business_started_pl::INT
        ELSE NULL
    END AS business_age_years

FROM {{ ref('staging_sl_case_table') }}
WHERE placement_opportunity_pl IS NOT NULL
