{{ 
    config(
        materialized='table',
        tags=['staging', 'sl_staging', 'dignity_kit', 'sl'],
        unique_key='case_id'
    ) 
}}

with raw_cases as (
    select
        data ->> 'case_id' as case_id,
        data -> 'properties' ->> 'date_opened' as date_opened_raw,
        data -> 'properties' ->> 'case_name' as case_name,
        data -> 'properties' ->> 'Beneficiary_Name' as beneficiary_name,
        data -> 'properties' ->> 'Age' as age_text,
        data -> 'properties' ->> 'DOB' as dob_text,
        data -> 'properties' ->> 'gender' as gender_raw,
        data -> 'properties' ->> 'county' as county,
        data -> 'properties' ->> 'Sub-county' as subcounty,
        data -> 'properties' ->> 'ward' as ward,
        data -> 'properties' ->> 'telephone_number' as telephone_number,
        data -> 'properties' ->> 'national_id_number' as national_id_number,
        data -> 'properties' ->> 'Parent' as parent_flag,
        data -> 'properties' ->> 'PWD' as pwd_flag,
        data -> 'properties' ->> 'Head_of_the_HH' as head_of_household,
        data -> 'properties' ->> 'HH_members_engaged_in_income_generating_activity' as hh_members_iga,
        data -> 'properties' ->> 'Oftenness_in_the_reduction_of_meals_in_the_last_3_months' as meals_reduction_freq,
        data -> 'properties' ->> 'Forego_basic_essentials' as forego_basic_essentials,
        data -> 'properties' ->> 'Last_3_months_received_or_receiving_support_from_SHOFCO' as support_from_shofco,
        data -> 'properties' ->> 'Intervention_they_are_under' as intervention,
        data -> 'properties' ->> 'Activity' as activity,
        data -> 'properties' ->> 'Name_of_TVET' as name_of_tvet,
        data -> 'properties' ->> 'gender_of_hh_head' as gender_of_hh_head,
        data -> 'properties' ->> 'participants_refugee_type_dir' as participants_refugee_type_dir,
        data -> 'properties' ->> 'refugee_type_dir' as refugee_type_dir,
        data -> 'properties' ->> 'overal_total_score_analysis' as total_score_raw
    from {{ source('staging_sl', 'zzz_case') }}
    where data -> 'properties' ->> 'case_type' = 'Dignity_Kit_Criteria'
),
prepared as (
    select
        case_id,
        {{ validate_date('date_opened_raw') }} as date_opened,
        case_name,
        beneficiary_name,
        age_text,
        {{ validate_date('dob_text') }} as dob,
        {{ normalize_gender('gender_raw') }} as gender,
        county,
        subcounty,
        ward,
        telephone_number,
        national_id_number,
        parent_flag,
        pwd_flag,
        head_of_household,
        hh_members_iga,
        meals_reduction_freq,
        forego_basic_essentials,
        support_from_shofco,
        intervention,
        activity,
        name_of_tvet,
        gender_of_hh_head,
        coalesce(participants_refugee_type_dir, refugee_type_dir) as refugee_type,
        case
            when total_score_raw ~ '^[0-9]+(\\.[0-9]+)?$' then total_score_raw::numeric
            else null
        end as total_score_reported
    from raw_cases
),
-- CommCare can emit more than one properties blob per case_id (e.g. a case update
-- event alongside the original open event). Since this model claims unique_key='case_id'
-- but is materialized='table' (unique_key only applies to incremental models, so it was
-- being silently ignored), we enforce the uniqueness ourselves here by collapsing all
-- rows sharing a case_id into one, keeping the most complete non-null value per column
-- rather than arbitrarily discarding whichever duplicate row we don't pick.
deduplicated as (
    select
        case_id,
        max(date_opened) as date_opened,
        max(nullif(trim(case_name), '')) as case_name,
        max(nullif(trim(beneficiary_name), '')) as beneficiary_name,
        max(nullif(trim(age_text), '')) as age_text,
        max(dob) as dob,
        max(nullif(trim(gender), '')) as gender,
        max(nullif(trim(county), '')) as county,
        max(nullif(trim(subcounty), '')) as subcounty,
        max(nullif(trim(ward), '')) as ward,
        max(nullif(trim(telephone_number), '')) as telephone_number,
        max(nullif(trim(national_id_number), '')) as national_id_number,
        max(nullif(trim(parent_flag), '')) as parent_flag,
        max(nullif(trim(pwd_flag), '')) as pwd_flag,
        max(nullif(trim(head_of_household), '')) as head_of_household,
        max(nullif(trim(hh_members_iga), '')) as hh_members_iga,
        max(nullif(trim(meals_reduction_freq), '')) as meals_reduction_freq,
        max(nullif(trim(forego_basic_essentials), '')) as forego_basic_essentials,
        max(nullif(trim(support_from_shofco), '')) as support_from_shofco,
        max(nullif(trim(intervention), '')) as intervention,
        max(nullif(trim(activity), '')) as activity,
        max(nullif(trim(name_of_tvet), '')) as name_of_tvet,
        max(nullif(trim(gender_of_hh_head), '')) as gender_of_hh_head,
        max(nullif(trim(refugee_type), '')) as refugee_type,
        max(total_score_reported) as total_score_reported
    from prepared
    group by case_id
),
-- Location fix: this form stores county/subcounty/ward as raw CommCare codes
-- (e.g. 'NBI', 'kisauni', 'port_reitz') rather than display names, unlike
-- staging_sl_case_table.sql which already normalizes location. We resolve
-- codes to real names here, matching the FULL hierarchy together (county_code +
-- constituency_id + ward_id) so a code that's individually valid but doesn't
-- belong with the other two on the same row still gets caught. Genuine gaps
-- in the lookup (e.g. dandora_area_iv / dandora_area_v, which the lookup only
-- has combined as dandora_area_iv_v) are intentionally left as the raw code
-- rather than guessed at, and flagged via location_lookup_status.
location_resolved as (
    select
        deduplicated.*,
        wl.county_name,
        wl.constituency_name,
        wl.ward_name,
        case
            when wl.county_id is null then 'not_found_in_lookup'
            else 'resolved'
        end as location_lookup_status
    from deduplicated
    left join {{ source('staging_gender', 'ward_lookup') }} wl
        on wl.county_code = deduplicated.county
       and wl.constituency_id = deduplicated.subcounty
       and wl.ward_id = deduplicated.ward
)

select
    case_id,
    date_opened,
    case_name,
    beneficiary_name,
    age_text,
    dob,
    gender,
    coalesce(county_name, county) as county,
    coalesce(constituency_name, subcounty) as subcounty,
    coalesce(ward_name, ward) as ward,
    location_lookup_status,
    telephone_number,
    national_id_number,
    parent_flag,
    pwd_flag,
    head_of_household,
    hh_members_iga,
    meals_reduction_freq,
    forego_basic_essentials,
    support_from_shofco,
    intervention,
    activity,
    name_of_tvet,
    gender_of_hh_head,
    refugee_type,
    total_score_reported
from location_resolved