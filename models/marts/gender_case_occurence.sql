with
    case_occurrences_data as (
        select
            case_id,
            {{ validate_date("date_of_reporting") }} as date_of_case_reporting,
            {{ validate_date("date_of_onboarding") }} as date_of_safe_house_onboarding,
            {{ validate_date("date_of_discharge") }} as date_of_safe_house_discharge,
            cast(
                to_char(date_of_reporting::date, 'YYYYMMDD') as integer
            ) as case_reporting_datekey,

            case
                when case_intake_date is null
                then {{ validate_date("date_of_reporting") }}
                else {{ validate_date("case_intake_date") }}
            end as date_of_case_intake,

            case
                when case_intake_date is null
                then cast(to_char(date_of_reporting::date, 'YYYYMMDD') as integer)
                else cast(to_char(case_intake_date::date, 'YYYYMMDD') as integer)
            end as case_intake_datekey,
            {{ validate_date("date_of_case_closure") }} as date_of_case_closure,
            {{ validate_date("case_assignment_date") }} as date_of_case_assignment,
            gender_site_code_of_reporting,
            where_was_the_client_referred_to as case_referred_to_location,
            case_number,
            assault_type,
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
                when (date_of_case_closure is null) then 'no' else 'yes'
            end as case_is_closed,
            case
                when
                    (
                        is_the_case_proceeding_to_court is not null
                        and date_of_case_closure is null
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
        from {{ ref("stg_gender_case_occurrences_commcare") }}
    )

select distinct
    cases.*,
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
    ) as case_county_name,
    coalesce(
        locations.constituency_name, cases.incident_report_constituency_code
    ) as case_constituency_name,
    coalesce(locations.ward_name, cases.incident_report_ward_code) as case_ward_name,
    gender_sites.site_name as gender_site_name_of_reporting
from case_occurrences_data cases
left join
    {{ ref("gender_survivors_data") }} survivors
    on cases.parent_case_id = survivors.case_id
left join
    {{ source("source_commcare", "dim_location_administrative_units") }} locations
    on cases.incident_report_county_code = locations.county_code
    and cases.incident_report_constituency_code = locations.constituency_id
    and cases.incident_report_ward_code = locations.ward_id
left join
    {{ source("source_commcare", "dim_gender_sites") }} gender_sites
    on cases.gender_site_code_of_reporting = gender_sites.site_code