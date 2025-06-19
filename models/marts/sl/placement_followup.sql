{{ config(
  materialized='table',
  tags=['placement_followup', 'sl']
) }}

SELECT
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
    average_income_in_kes,
    date_received,

    -- Renamed fields for clarity
    new_role_source_pl AS new_role_source,
    documents_held_pl AS documents_held,
    sector_pl AS organization_sector,
    employment_type_pl AS nature_of_employment,

    -- 🆕 Business and employee-related fields
    business_start_date_pl,
    year_business_started_pl::INT AS year_business_started,
    has_employees_pl,
    number_of_employees_pl,
    female_employees_pl,
    avg_employees_pl,
    avg_female_employees_pl,

    -- 🧮 Business age calculation (in years, from year_business_started)
    CASE 
        WHEN year_business_started_pl ~ '^\d{4}$' 
        THEN EXTRACT(YEAR FROM CURRENT_DATE) - year_business_started_pl::INT
        ELSE NULL
    END AS business_age_years

FROM {{ ref('staging_sl_placement_followup') }}
