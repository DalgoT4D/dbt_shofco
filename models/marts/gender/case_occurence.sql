{{ config(
  materialized='table',
  tags= ["commcare_extraction", "gender_cases", "gender"]
) }}

with
safe_house_data as (
    select
        case_id,
        parent_case_id,
        {{ validate_date("date_of_safehouse_onboarding") }} as date_of_safe_house_onboarding,
        {{ validate_date("date_of_discharge") }} as date_of_safe_house_discharge
    from {{ ref("staging_gender_safe_house_commcare") }}
    where {{ validate_date("date_of_safehouse_onboarding") }} is not NULL
),

case_occurrences_data as (
    select
        case_id,
        assigned_to,
        previous_case_number,
        {{ validate_date("date_of_reporting") }} as date_of_case_reporting,
        cast(
            to_char(cast(date_of_reporting as date), 'YYYYMMDD') as integer
        ) as case_reporting_datekey,

        case
            when case_intake_date is NULL
                then {{ validate_date("date_of_reporting") }}
            else {{ validate_date("case_intake_date") }}
        end as date_of_case_intake,

        case
            when case_intake_date is NULL
                then cast(to_char(cast(date_of_reporting as date), 'YYYYMMDD') as integer)
            else cast(to_char(cast(case_intake_date as date), 'YYYYMMDD') as integer)
        end as case_intake_datekey,
        {{ validate_date("date_of_case_closure") }} as date_of_case_closure,
        {{ validate_date("case_assignment_date") }} as date_of_case_assignment,
        case 
            -- Only apply calculation if case_intake_date is not null
            when {{ validate_date("case_intake_date") }} is not NULL
                then 
                    -- For closed cases, calculate duration between intake and closure
                    case 
                        when {{ validate_date("date_of_case_closure") }} is not NULL
                            then 
                                (cast({{ validate_date("date_of_case_closure") }} as date) - cast({{ validate_date("case_intake_date") }} as date))
                        -- For ongoing cases, calculate duration from intake to today
                        else 
                            (current_date - cast({{ validate_date("case_intake_date") }} as date))
                    end
        end as case_duration_in_days,
        gender_site_code_of_reporting,
        where_was_the_client_referred_to as case_referred_to_location,
        assault_type,
        case
            when assault_type ilike '%juvenile%' then 'Juvenile Case'
            when assault_type ilike '%domestic%' then 'Domestic Violence'
            when assault_type ilike '%sexual%' then 'Sexual Violence'
            when assault_type ilike '%physical%' then 'Physical Violence'
            when assault_type ilike '%negligence%' then 'Negligence'
            when assault_type ilike '%legal%' or assault_type ilike '%counsel%' then 'Seeking Legal Counsel'
            else 'Other'
        end as cleaned_assault_type,

        -- Split assault types into multiple rows
        -- unnest(string_to_array(assault_type, ' ')) as assault_type,

        case
            when where_was_the_client_referred_to like '%safe_house%'
                then 'yes'
            else 'no'
        end as referred_to_safe_house,
        case
            when where_was_the_client_referred_to like '%other_shofco_programs%'
                then 'yes'
            else 'no'
        end as referred_to_other_shofco_programs,
        case
            when where_was_the_client_referred_to like '%district_children_officers%'
                then 'yes'
            else 'no'
        end as referred_to_dco,
        case
            when
                where_was_the_client_referred_to
                like '%psychosocial_support_and_counseling%'
                then 'yes'
            else 'no'
        end as referred_to_counseling_and_support,
        case
            when where_was_the_client_referred_to like '%police%'
                then 'yes'
            else 'no'
        end as referred_to_police,
        case
            when where_was_the_client_referred_to like '%medical%'
                then 'yes'
            else 'no'
        end as referred_for_medical_intervention,
        village_of_incident_report as incident_report_village_name,
        ward_of_incident_report as incident_report_ward_code,
        constituency_of_incident_report as incident_report_constituency_code,
        county_of_incident_report as incident_report_county_code,
        case
            when (date_of_case_closure is NULL) then 'no' else 'yes'
        end as case_is_closed,
        is_the_case_proceeding_to_court,
        case
            when
                (
                    is_the_case_proceeding_to_court is not NULL
                    and date_of_case_closure is NULL
                )
                then 'yes'
            else 'no'
        end as case_still_in_court,
        {{ validate_date("date_of_court_followup") }} as date_of_court_followup,
        at_what_stage_is_the_court_case_currently_at as stage_of_case_in_court,
        case_reported_to_police,
        medium_of_reporting,
        parent_case_id,
        parent_case_type,
        closed
    from {{ ref("staging_gender_case_occurrences_commcare") }}
)

select distinct
    cases.*,
    safe_house_data.date_of_safe_house_onboarding,
    safe_house_data.date_of_safe_house_discharge,
    current_date - cases.date_of_case_intake as days_since_intake,
    survivors.gender as survivor_gender,
    survivors.age as survivor_age,
    case
        when survivors.age < 15
            then '<15 years'
        when survivors.age >= 15 and survivors.age <= 24
            then '15 - 24 years'
        when survivors.age >= 25 and survivors.age <= 54
            then '25 - 54 years'
        when survivors.age >= 55 and survivors.age <= 64
            then '55 - 64 years'
        when survivors.age >= 65
            then '65 years and above'
        else 'Unknown'
    end as age_group,
    coalesce(
        locations.county_name, cases.incident_report_county_code
    ) as county,
    coalesce(
        locations.constituency_name, cases.incident_report_constituency_code
    ) as case_constituency_name,
    coalesce(locations.ward_name, cases.incident_report_ward_code) as case_ward_name,
    gender_sites.site_name as site,
    case when case_referred_to_location is NULL then 'Yes' else 'No' end as is_case_referred
from case_occurrences_data as cases
left join
    safe_house_data
    on cases.case_id = safe_house_data.parent_case_id
left join
    {{ ref("survivors_data") }} as survivors
    on cases.parent_case_id = survivors.case_id
left join
    {{ source("staging_gender", "dim_location_administrative_units") }} as locations
    on
        cases.incident_report_county_code = locations.county_code
        and cases.incident_report_constituency_code = locations.constituency_id
        and cases.incident_report_ward_code = locations.ward_id
left join
    {{ source("staging_gender", "dim_gender_sites") }} as gender_sites
    on cases.gender_site_code_of_reporting = gender_sites.site_code
