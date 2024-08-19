
with case_occurrences_data as (
    SELECT
	case_id,
	{{ validate_date('date_of_reporting') }} as date_of_case_reporting, 
	{{ validate_date('date_of_onboarding') }} as date_of_safe_house_onboarding, 
	{{ validate_date('date_of_discharge') }} as date_of_safe_house_discharge, 
    CAST(TO_CHAR(date_of_reporting::date,'YYYYMMDD') AS INTEGER)  as case_reporting_datekey,

	CASE 
		WHEN case_intake_date IS NULL THEN {{ validate_date('date_of_reporting') }}
		 ELSE {{ validate_date('case_intake_date') }} 
		END as date_of_case_intake, 
    
	CASE 
		WHEN case_intake_date IS NULL THEN CAST(TO_CHAR(date_of_reporting::date,'YYYYMMDD') AS INTEGER)
		ELSE CAST(TO_CHAR(case_intake_date::date,'YYYYMMDD') AS INTEGER)
	END as case_intake_datekey,
	{{ validate_date('date_of_case_closure') }} as date_of_case_closure,
	{{ validate_date('case_assignment_date') }} as date_of_case_assignment,
	gender_site_code_of_reporting,
	where_was_the_client_referred_to as case_referred_to_location,
	case_number,
    assault_type,
	CASE 
		WHEN where_was_the_client_referred_to LIKE '%safe_house%' THEN 'yes'
		ELSE 'no' 
	END AS referred_to_safe_house,
	CASE 
		WHEN where_was_the_client_referred_to LIKE '%other_shofco_programs%' THEN 'yes'
		ELSE 'no' 
	END AS referred_to_other_shofco_programs,
	CASE 
		WHEN where_was_the_client_referred_to LIKE '%psychosocial_support_and_counseling%' THEN 'yes'
		ELSE 'no' 
	END AS referred_to_counseling_and_support,
	CASE 
		WHEN where_was_the_client_referred_to LIKE '%police%' THEN 'yes'
		ELSE 'no' 
	END AS referred_to_police,
	CASE 
		WHEN where_was_the_client_referred_to LIKE '%medical%' THEN 'yes'
		ELSE 'no' 
	END AS referred_for_medical_intervention,
	village_of_incident_report AS incident_report_village_name,
	ward_of_incident_report AS incident_report_ward_code,
	constituency_of_incident_report AS incident_report_constituency_code,
	county_of_incident_report AS incident_report_county_code,
	CASE 
		WHEN (date_of_case_closure IS NULL) THEN 'no'
		ELSE 'yes'
	END AS case_is_closed,
	CASE 
		WHEN (is_the_case_proceeding_to_court IS NOT NULL AND date_of_case_closure IS NULL) 
			THEN 'yes'
		ELSE 'no'
	END AS case_still_in_court,
	{{ validate_date('date_of_court_followup') }} as date_of_court_followup, 
	at_what_stage_is_the_court_case_currently_at as stage_of_case_in_court,
    case_reported_to_police,
	medium_of_reporting,
	parent_case_id,
	parent_case_type,
	closed
FROM {{ref('stg_gender_case_occurrences_commcare')}}
)

SELECT distinct cases.*,
survivors.gender as survivor_gender,
survivors.age as survivor_age,
CASE
    WHEN survivors.age < 15 THEN '<15 years'
    WHEN survivors.age >= 15 AND survivors.age <= 24 THEN '15 - 24 years'
    WHEN survivors.age >= 25 AND survivors.age <= 54 THEN '25 - 54 years'
    WHEN survivors.age >= 55 AND survivors.age <= 64 THEN '55 - 64 years'
    WHEN survivors.age >= 65 THEN '65 years and above'
    ELSE 'Unknown'
END AS age_group,
COALESCE(locations.county_name, cases.incident_report_county_code) as case_county_name,
COALESCE(locations.constituency_name,cases.incident_report_constituency_code) as case_constituency_name,
COALESCE(locations.ward_name,cases.incident_report_ward_code) as case_ward_name,
gender_sites.site_name as gender_site_name_of_reporting
FROM case_occurrences_data cases
LEFT JOIN {{ ref('gender_survivors_data') }} survivors
ON cases.parent_case_id = survivors.case_id
LEFT JOIN {{ source('source_commcare','dim_location_administrative_units') }} locations
ON cases.incident_report_county_code = locations.county_code
AND cases.incident_report_constituency_code = locations.constituency_id
AND cases.incident_report_ward_code = locations.ward_id
LEFT JOIN {{ source('source_commcare','dim_gender_sites') }} gender_sites
ON cases.gender_site_code_of_reporting = gender_sites.site_code