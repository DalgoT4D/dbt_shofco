{{
    config(
        materialized='incremental',
        unique_key='case_id',
        schema='staging_sl',
        alias='sl_tvet_enrolment',
        tags=['commcare_extraction', 'sl_cases', 'sustainable_livelihoods', 'tvet']
    )
}}

{% set properties_dict = {
    "owner_id": "owner_id",
    "external_id": "external_id",
    "parent_name": "parent_name",
    "tvet_cohort": "tvet_cohort",
    "officer_name": "officer_name",
    "registered_by": "registered_by",
    "tvet_start_date": "tvet_start_date",
    "tvet_completion_date": "tvet_completion_date",
    "tvet_course_enrolled": "tvet_course_enrolled",
    "tvet_participant_program": "tvet_participant_program",
    "name_of_tvet_school_placed": "tvet_school_placed",
    "select_county_of_school_placed": "county_of_school_placed",
    "other_institution": "other_institution",
    "parent_phone_number": "parent_phone_number",
    "specify_tvet_course": "specified_course",
    "date_of_registration": "date_of_registration"
} %}

{{ extract_case_table_from_sl_commcare_json('sl_tvet_enrolment_case_data', properties_dict) }}
