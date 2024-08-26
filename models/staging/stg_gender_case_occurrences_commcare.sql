{{
    config(
        materialized="table",
        unique_key="case_id",
        alias="stg_gender_case_occurrences_commcare",
        tags="commcare_extraction",
    )
}}

{% set commcare_case_type = "case_occurrences_case_data" %}
{% set case_type_properties_dict = {
    "acknowledgement_of_informed_consent": "acknowledgement_of_informed_consent",
    "assault_type": "assault_type",
    "assigned_to": "assigned_to",
    "at_what_stage_is_the_court_case_currently_at": "at_what_stage_is_the_court_case_currently_at",
    "case_assignment_date": "case_assignment_date",
    "case_closure_other_reason": "case_closure_other_reason",
    "case_closure_reason": "case_closure_reason",
    "case_intake_date": "case_intake_date",
    "case_number": "case_number",
    "case_summary_notes": "case_summary_notes",
    "case_reassignment_date": "case_reassignment_date",
    "case_reported_to_police": "case_reported_to_police",
    "case_worker_date_of_onboarding": "case_worker_date_of_onboarding",
    "consent_for_information_release": "consent_for_information_release",
    "constituency_of_incident_report": "constituency_of_incident_report",
    "county_of_incident_report": "county_of_incident_report",
    "court_followup_comments": "court_followup_comments",
    "date_consent_was_amended": "date_consent_was_amended",
    "date_consent_was_given": "date_consent_was_given",
    "date_of_birth_of_next_of_kin": "date_of_birth_of_next_of_kin",
    "date_of_birth_of_well_wisher": "date_of_birth_of_well_wisher",
    "date_of_case_closure": "date_of_case_closure",
    "date_of_consent_being_captured": "date_of_consent_being_captured",
    "date_of_counselling_registration": "date_of_counselling_registration",
    "date_of_court_followup": "date_of_court_followup",
    "date_of_court_followup_amended_at": "date_of_court_followup_amended_at",
    "date_of_court_followup_update": "date_of_court_followup_update",
    "date_of_medical_followup_amendment": "date_of_medical_followup_amendment",
    "date_of_medical_followup_update": "date_of_medical_followup_update",
    "date_of_medical_report_followup": "date_of_medical_report_followup",
    "date_of_onboarding": "date_of_onboarding",
    "date_of_discharge": "date_of_discharge",
    "date_of_other_followup_amended_at": "date_of_other_followup_amended_at",
    "date_of_other_followup_update": "date_of_other_followup_update",
    "date_of_police_followup_last_amended_at": "date_of_police_followup_last_amended_at",
    "date_of_police_followup_update": "date_of_police_followup_update",
    "date_of_police_report_followup": "date_of_police_report_followup",
    "date_of_reporting": "date_of_reporting",
    "date_of_updating_case_occurrence_information": "date_of_updating_case_occurrence_information",
    "date_referral_information_captured": "date_referral_information_captured",
    "date_referral_information_last_amended": "date_referral_information_last_amended",
    "gender_of_next_of_kin": "gender_of_next_of_kin",
    "gender_of_well_wisher": "gender_of_well_wisher",
    "gender_site_code_of_reporting": "gender_site_code_of_reporting",
    "has_the_client_undergone_other_procedures": "has_the_client_undergone_other_procedures",
    "have_all_witnesses_reocrded_their_statements": "have_all_witnesses_reocrded_their_statements",
    "if_some_witnesses_have_not_recorded_their_statements_what_action_was_taken": "if_some_witnesses_have_not_recorded_their_statements_what_action_was_taken",
    "is_the_case_proceeding_to_court": "is_the_case_proceeding_to_court",
    "is_the_case_still_pending_at_the_police_station": "is_the_case_still_pending_at_the_police_station",
    "medical_report_filed": "medical_report_filed",
    "medium_of_reporting": "medium_of_reporting",
    "other_assault_type": "other_assault_type",
    "other_reason": "other_reason",
    "other_recipient_for_information": "other_recipient_for_information",
    "other_referral_location": "other_referral_location",
    "police_followup_comments": "police_followup_comments",
    "police_station_where_the_case_was_reported": "police_station_where_the_case_was_reported",
    "previous_case_number": "previous_case_number",
    "reason_for_case_pending_at_station": "reason_for_case_pending_at_station",
    "recipients_of_information_release": "recipients_of_information_release",
    "referral_reason": "referral_reason",
    "reporter_type": "reporter_type",
    "sexual_assault_type": "sexual_assault_type",
    "village_of_incident_report": "village_of_incident_report",
    "ward_of_incident_report": "ward_of_incident_report",
    "was_there_a_witness_to_the_incident": "was_there_a_witness_to_the_incident",
    "what_is_the_age_provided": "what_is_the_age_provided",
    "what_is_the_year_of_birth_of_survivor": "what_is_the_year_of_birth_of_survivor",
    "what_other_procedures_has_the_client_undergone": "what_other_procedures_has_the_client_undergone",
    "where_was_the_client_referred_to": "where_was_the_client_referred_to",
} -%}


with case_cte as ({{
    extract_case_table_from_commcare_json(
        commcare_case_type, case_type_properties_dict, true
    )
}})

{{ dbt_utils.deduplicate(
    relation='case_cte',
    partition_by='case_id',
    order_by='indexed_on desc',
   )
}}
