{{ 
    config(
        materialized='table',
        unique_key='case_id',
        tags=['staging', 'sl_staging', 'commcare_extraction', 'sl']
    ) 
}}

with raw_cases as (
    select
        data ->> 'case_id' as case_id,
        COALESCE(data -> 'properties' ->> 'date_of_registration', data -> 'properties' ->> 'date_of_registration_dir') as date_of_registration_raw,
        data -> 'properties' ->> 'pp_unique_id' as pp_unique_id,
        coalesce(
            data -> 'properties' ->> 'participant_full-name_dir',
            data -> 'properties' ->> 'pp_fullname',
            data -> 'properties' ->> 'pp_fullname_dir',
            data -> 'properties' ->> 'name_sp',
            data -> 'properties' ->> 'name_sr',
            data -> 'properties' ->> 'name_tc',
            data -> 'properties' ->> 'name_pl',
            data -> 'properties' ->> 'name_psr',
            data -> 'properties' ->> 'users_fullname',
            data -> 'properties' ->> 'fullname',
            data -> 'properties' ->> 'fullname_csr'
        ) as pp_fullname,
        data -> 'properties' ->> 'profession_of_facilitator_psr' as profession_of_facilitator_psr,
        data -> 'properties' ->> 'psychosocial_facilitator_psr' as psychosocial_facilitator_psr,
        coalesce(
            data -> 'properties' ->> 'pp_sex',
            data -> 'properties' ->> 'sex_csf',
            data -> 'properties' ->> 'sex_dir',
            data -> 'properties' ->> 'sex_uid',
            data -> 'properties' ->> 'gender_bm',
            data -> 'properties' ->> 'gender_csf',
            data -> 'properties' ->> 'gender_hh_dka'
        ) as gender_raw,
        COALESCE(
            data -> 'properties' ->> 'do_you_have_children_sr',
            data -> 'properties' ->> 'do_you_have_children_uf'
        ) as do_you_have_children,
        coalesce(
            data -> 'properties' ->> 'nationality_csf',
            data -> 'properties' ->> 'nationality_dir'
        ) as nationality,
        coalesce(
            data -> 'properties' ->> 'participants_refugee_type_dir',
            data -> 'properties' ->> 'refugee_type_dir'
        ) as refugee_type,
        data -> 'properties' ->> 'kenyan_national_id_number_dir' as kenyan_national_id_number_dir,
        coalesce(
            data -> 'properties' ->> 'county_dir',
            data -> 'properties' ->> 'participants_county_of_residence_dir',
            data -> 'properties' ->> 'pp_shofco_county',
            data -> 'properties' ->> 'county_bm',
            data -> 'properties' ->> 'county_int',
            data -> 'properties' ->> 'coworking_county_csr'
        ) AS county,
        coalesce(
            data -> 'properties' ->> 'pp_shofco_subcounty',
            data -> 'properties' ->> 'subcounty_apprenticeship_provider_apr',
            data -> 'properties' ->> 'subcounty_bm',
            data -> 'properties' ->> 'subcounty_business_bga',
            data -> 'properties' ->> 'subcounty_business_located_bga',
            data -> 'properties' ->> 'subcounty_coworking_site_csf',
            data -> 'properties' ->> 'sub_county_of_institution_ttia',
            data -> 'properties' ->> 'sub-county_of_shofco_site_dir',
            data -> 'properties' ->> 'subcounty_shofco_site_dir',
            data -> 'properties' ->> 'coworking_subcounty_csr'
        ) as subcounty,
        coalesce(
            data -> 'properties' ->> 'pp_ahofco_ward',
            data -> 'properties' ->> 'ward_bm',
            data -> 'properties' ->> 'ward_business_bga',
            data -> 'properties' ->> 'ward_business_located_bga',
            data -> 'properties' ->> 'ward_coworking_site_csf',
            data -> 'properties' ->> 'ward_dc',
            data -> 'properties' ->> 'ward_dir',
            data -> 'properties' ->> 'ward_located_apbl',
            data -> 'properties' ->> 'ward_of_shofco_site_dir',
            data -> 'properties' ->> 'ward_of_the_institution_ttia',
            data -> 'properties' ->> 'ward_shofco_site_dir',
            data -> 'properties' ->> 'coworking_ward_csr'
        ) as ward,
        data -> 'properties' ->> 'coworking_county_csr' as coworking_county_csr,
        data -> 'properties' ->> 'coworking_subcounty_csr' as coworking_subcounty_csr,
        data -> 'properties' ->> 'coworking_ward_csr' as coworking_ward_csr,
        coalesce(
            data -> 'properties' ->> 'primary_phone_number_dir',
            data -> 'properties' ->> 'primary_phone_number_csf'
        ) as primary_phone_number,
        coalesce(
            data -> 'properties' ->> 'is_plwd',
            data -> 'properties' ->> 'is_plwd_csf',
            data -> 'properties' ->> 'is_pwd',
            data -> 'properties' ->> 'is_pwd_csf',
            data -> 'properties' ->> 'is_pwd_tvbl'
        ) as is_pwd,
        coalesce(
            data -> 'properties' ->> 'type_of_disability_dir',
            data -> 'properties' ->> 'type_of_disability',
            data -> 'properties' ->> 'type_of_disability_tvbl'
        ) as type_of_disability_dir,
        data -> 'properties' ->> 'apprenticeship_provider_apr' as apprenticeship_provider_apr,
        data -> 'properties' ->> 'skill_enrolled_apr' as skill_enrolled_apr,
        data -> 'properties' ->> 'placement_date_apr' as placement_date_apr_raw,
        data -> 'properties' ->> 'grant_amount_bg' as grant_amount_bg,
        data -> 'properties' ->> 'type_of_business_you_operate_bga' as type_of_business_you_operate_bga,
        data -> 'properties' ->> 'mark_the_location_of_the_business' as mark_the_location_of_the_business_raw,
        data -> 'properties' ->> 'gps_of_business_location_apbl' as gps_of_business_location_apbl_raw,
        data -> 'properties' ->> 'location_of_institution_ttia' as location_of_institution_ttia_raw,
        data -> 'properties' ->> 'date_grant_allocated_bg' as date_grant_allocated_bg_raw,
        data -> 'properties' ->> 'digital_literacy_dl' as digital_literacy_dl,
        data -> 'properties' ->> 'start_date_dl' as start_date_dl_raw,
        data -> 'properties' ->> 'advanced_it_start_date_dl' as advanced_it_start_date_dl_raw,
        data -> 'properties' ->> 'advanced_it_completion_date_dl' as advanced_it_completion_date_dl_raw,
        data -> 'properties' ->> 'completion_date_dl' as completion_date_dl_raw,
        data -> 'properties' ->> 'final_exams_tc' as final_exams_tc,
        data -> 'properties' ->> 'dignity_package_helpful_dka' as dignity_package_helpful_dka,
        data -> 'properties' ->> 'how_helpful_course' as how_helpful_course,
        data -> 'properties' ->> 'how_helpful_course_tc' as how_helpful_course_tc,
        data -> 'properties' ->> 'how_helpful_was_upskilling_uc' as how_helpful_was_upskilling_uc,
        data -> 'properties' ->> 'Why_not_helpful_tc' as why_not_helpful_tc,
        data -> 'properties' ->> 'why_not_helpful_uc' as why_not_helpful_uc,
        data -> 'properties' ->> 'how_helpful_course_tvet' as how_helpful_course_tvet,
        data -> 'properties' ->> 'recommend_training_tvet' as recommend_training_tvet,
        data -> 'properties' ->> 'start_date_ent' as start_date_ent_raw,
        data -> 'properties' ->> 'completion_date_ent' as completion_date_ent_raw,
        data -> 'properties' ->> 'interest_in_sales_work_ent' as interest_in_sales_work_ent,
        data -> 'properties' ->> 'support_to_help_succeed_as_entrepreneur_sr' as support_to_help_succeed_as_entrepreneur_sr,
        data -> 'properties' ->> 'start_date_int' as start_date_int_raw,
        data -> 'properties' ->> 'completion_date_int' as completion_date_int_raw,
        data -> 'properties' ->> 'income_on_average_pl' as income_on_average_pl,
        data -> 'properties' ->> 'placement_opportunity_pl' as placement_opportunity_pl,
        data -> 'properties' ->> 'date_first_visit_bm' as date_first_visit_bm_raw,
        data -> 'properties' ->> 'date_of_second_visit_bm' as date_of_second_visit_bm_raw,
        data -> 'properties' ->> 'date_of_visit_csf' as date_of_visit_csf_raw,
        data -> 'properties' ->> 'date_of_visit_csr' as date_of_visit_csr_raw,
        data -> 'properties' ->> 'date_of_birth_csf' as date_of_birth_csf,
        data -> 'properties' ->> 'age_in_years_csf' as age_in_years_csf,
        data -> 'properties' ->> 'first_name_csf' as first_name_csf,
        data -> 'properties' ->> 'second_name_csf' as second_name_csf,
        data -> 'properties' ->> 'how_many_csf' as how_many_csf,
        data -> 'properties' ->> 'time_in_csf' as time_in_csf,
        data -> 'properties' ->> 'time_in_csr' as time_in_csr,
        data -> 'properties' ->> 'time_out_csf' as time_out_csf,
        data -> 'properties' ->> 'time_out_csr' as time_out_csr,
        data -> 'properties' ->> 'using_coworking_for_a_living_csr' as using_coworking_for_a_living_csr,
        data -> 'properties' ->> 'using_coworking_to_earn_a_living_csf' as using_coworking_to_earn_a_living_csf,
        data -> 'properties' ->> 'what_visitor_is_using_csf' as what_visitor_is_using_csf,
        data -> 'properties' ->> 'what_visitor_is_using_csr' as what_visitor_is_using_csr,
        data -> 'properties' ->> 'purpose_for_using_csf' as purpose_for_using_csf,
        data -> 'properties' ->> 'coworking_space_facility' as coworking_space_facility,
        data -> 'properties' ->> 'swep_handcraft_center-hc' as swep_handcraft_center_hc,
        data -> 'properties' ->> 'start_date_hc' as start_date_hc_raw,
        data -> 'properties' ->> 'completion_date_hc' as completion_date_hc_raw,
        coalesce(
            data -> 'properties' ->> 'handcraft_training_type_hc',
            data -> 'properties' ->> 'handcraft_training_type_sr'
        ) as handcraft_training_type_hc,
        data -> 'properties' ->> 'swep-iga-center-iga' as swep_iga_center_iga,
        data -> 'properties' ->> 'start_date_iga' as start_date_iga_raw,
        data -> 'properties' ->> 'completion_date_iga' as completion_date_iga_raw,
        data -> 'properties' ->> 'training_type_iga' as training_type_iga,
        data -> 'properties' ->> 'swep_tailoring_center_st' as swep_tailoring_center_st,
        data -> 'properties' ->> 'start_date_of_training_st' as start_date_of_training_st_raw,
        data -> 'properties' ->> 'expected_end_date_of_training_st' as expected_end_date_of_training_st_raw,
        coalesce(
            data -> 'properties' ->> 'training_session_st',
            data -> 'properties' ->> 'training_session_sr'
        ) as training_session_st,
        data -> 'properties' ->> 'name_of_institution_tvet' as name_of_institution_tvet,
        data -> 'properties' ->> 'name_of_facilitator' as name_of_facilitator,
        coalesce(
        data -> 'properties' ->> 'course_enrolled_tvet',
        data -> 'properties' ->> 'other_course_tvet'
        ) as course_enrolled_tvet,
        data -> 'properties' ->> 'start_date_tvet' as start_date_tvet_raw,
        case
            when coalesce(data -> 'properties' ->> 'completion_date_tvet',
                          data -> 'properties' ->> 'completion_tvet') in ('31/4/2025', '31/04/2025') then null
            else coalesce(
                data -> 'properties' ->> 'completion_date_tvet',
                data -> 'properties' ->> 'completion_tvet'
            )
        end as completion_date_tvet_raw,
        coalesce(
            data -> 'properties' ->> 'nita_exams',
            data -> 'properties' ->> 'nita_exams_tc'
        ) as nita_exams
    from {{ source('staging_sl', 'zzz_case') }}
    where data -> 'properties' ->> 'case_type' = 'sl_new'
),
prepared_cases as (
select
    case_id,
        {{ validate_date('date_of_registration_raw') }} as date_of_registration,
        pp_unique_id,
        pp_fullname,
        profession_of_facilitator_psr,
        psychosocial_facilitator_psr,
        {{ normalize_gender('gender_raw') }} as gender,
        do_you_have_children,
        nationality,
        refugee_type,
        kenyan_national_id_number_dir,
        county,
        subcounty,
        ward,
        coworking_county_csr,
        coworking_subcounty_csr,
        coworking_ward_csr,
        primary_phone_number,
        right(primary_phone_number, 8) as phone_last_8_digits,
        is_pwd,
        type_of_disability_dir,
        case
            when lower(trim({{ normalize_gender('gender_raw') }})) = 'female' and lower(trim(do_you_have_children)) = 'yes' then 'yes'
            when {{ normalize_gender('gender_raw') }} is null or trim({{ normalize_gender('gender_raw') }}) = '' or do_you_have_children is null or trim(do_you_have_children) = '' then 'don''t know'
            when lower(trim(do_you_have_children)) = 'no' then 'no'
            else 'no'
        end as is_young_mother,
        apprenticeship_provider_apr,
        type_of_business_you_operate_bga,
        skill_enrolled_apr,
        {{ validate_date('placement_date_apr_raw') }} as placement_date_apr,
        grant_amount_bg,
        {{ validate_date('date_grant_allocated_bg_raw') }} as date_grant_allocated_bg,
mark_the_location_of_the_business_raw,

        case 
            when mark_the_location_of_the_business_raw is not null
            then split_part(mark_the_location_of_the_business_raw, ' ', 1)::double precision
        end as business_latitude,

        case 
            when mark_the_location_of_the_business_raw is not null
            then split_part(mark_the_location_of_the_business_raw, ' ', 2)::double precision
        end as business_longitude,

        case 
            when mark_the_location_of_the_business_raw is not null
            then split_part(mark_the_location_of_the_business_raw, ' ', 3)::double precision
        end as business_altitude,

        case 
            when mark_the_location_of_the_business_raw is not null
            then split_part(mark_the_location_of_the_business_raw, ' ', 4)::double precision
        end as business_accuracy,

        gps_of_business_location_apbl_raw,

        case
            when gps_of_business_location_apbl_raw is not null
            then split_part(gps_of_business_location_apbl_raw, ' ', 1)::double precision
        end as apprenticeship_latitude,

        case
            when gps_of_business_location_apbl_raw is not null
            then split_part(gps_of_business_location_apbl_raw, ' ', 2)::double precision
        end as apprenticeship_longitude,

        case
            when gps_of_business_location_apbl_raw is not null
            then split_part(gps_of_business_location_apbl_raw, ' ', 3)::double precision
        end as apprenticeship_altitude,

        case
            when gps_of_business_location_apbl_raw is not null
            then split_part(gps_of_business_location_apbl_raw, ' ', 4)::double precision
        end as apprenticeship_accuracy,

        location_of_institution_ttia_raw,

        case
            when location_of_institution_ttia_raw is not null
            then split_part(location_of_institution_ttia_raw, ' ', 1)::double precision
        end as tvet_latitude,

        case
            when location_of_institution_ttia_raw is not null
            then split_part(location_of_institution_ttia_raw, ' ', 2)::double precision
        end as tvet_longitude,

        case
            when location_of_institution_ttia_raw is not null
            then split_part(location_of_institution_ttia_raw, ' ', 3)::double precision
        end as tvet_altitude,

        case
            when location_of_institution_ttia_raw is not null
            then split_part(location_of_institution_ttia_raw, ' ', 4)::double precision
        end as tvet_accuracy,

        digital_literacy_dl,
        {{ validate_date('start_date_dl_raw') }} as start_date_dl,
        {{ validate_date('advanced_it_start_date_dl_raw') }} as advanced_it_start_date_dl,
        {{ validate_date('advanced_it_completion_date_dl_raw') }} as advanced_it_completion_date_dl,
        {{ validate_date('completion_date_dl_raw') }} as completion_date_dl,
        final_exams_tc,
        dignity_package_helpful_dka,
        how_helpful_course,
        how_helpful_course_tc,
        how_helpful_was_upskilling_uc,
        why_not_helpful_tc,
        why_not_helpful_uc,
        how_helpful_course_tvet,
        recommend_training_tvet,
        {{ validate_date('start_date_ent_raw') }} as start_date_ent,
        {{ validate_date('completion_date_ent_raw') }} as completion_date_ent,
        interest_in_sales_work_ent,
        {{ validate_date('start_date_int_raw') }} as start_date_int,
        {{ validate_date('completion_date_int_raw') }} as completion_date_int,
        income_on_average_pl,
        placement_opportunity_pl,
        {{ validate_date('date_first_visit_bm_raw') }} as date_first_visit_bm,
        {{ validate_date('date_of_second_visit_bm_raw') }} as date_of_second_visit_bm,
        {{ validate_date('date_of_visit_csf_raw') }} as date_of_visit_csf,
        {{ validate_date('date_of_visit_csr_raw') }} as date_of_visit_csr,
        date_of_birth_csf,
        age_in_years_csf,
        first_name_csf,
        second_name_csf,
        how_many_csf,
        time_in_csf,
        time_in_csr,
        time_out_csf,
        time_out_csr,
        using_coworking_for_a_living_csr,
        using_coworking_to_earn_a_living_csf,
        what_visitor_is_using_csf,
        what_visitor_is_using_csr,
        purpose_for_using_csf,
        coworking_space_facility,
        swep_handcraft_center_hc,
        {{ validate_date('start_date_hc_raw') }} as start_date_hc,
        {{ validate_date('completion_date_hc_raw') }} as completion_date_hc,
        handcraft_training_type_hc,
        swep_iga_center_iga,
        {{ validate_date('start_date_iga_raw') }} as start_date_iga,
        {{ validate_date('completion_date_iga_raw') }} as completion_date_iga,
        training_type_iga,
        swep_tailoring_center_st,
        {{ validate_date('start_date_of_training_st_raw') }} as start_date_of_training_st,
        {{ validate_date('expected_end_date_of_training_st_raw') }} as expected_end_date_of_training_st,
        training_session_st,
        name_of_institution_tvet,
        name_of_facilitator,
        course_enrolled_tvet,
        {{ validate_date('start_date_tvet_raw') }} as start_date_tvet,
        {{ validate_date('completion_date_tvet_raw') }} as completion_date_tvet,
        nita_exams,
        lower(trim(pp_unique_id)) as norm_pp_unique_id,
        regexp_replace(lower(coalesce(pp_fullname, '')), '\s+', ' ', 'g') as norm_pp_fullname,
        lower(trim(kenyan_national_id_number_dir)) as norm_kenyan_id,
        case
            when county is null or trim(county) = '' then null
            else
                case
                    when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'kiii' then 'kilifi'
                    when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'buia' then 'busia'
                    when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'homabay' then 'homa bay'
                    when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'kiumu' then 'kisumu'
                    when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'mombaa' then 'mombasa'
                    when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'uaingihu' then 'uain gihu'
                    when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'trannzoia' then 'tran nzoia'
                    when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'taitataveta' then 'taita taveta'
                    else lower(trim(county))
                end
        end as norm_county
    from raw_cases
),

deduplicated_cases as (
    select
        min(nullif(trim(case_id), '')) as case_id,
        max(date_of_registration) as date_of_registration,
        max(nullif(trim(pp_unique_id), '')) as pp_unique_id,
        max(nullif(trim(pp_fullname), '')) as pp_fullname,
        max(nullif(trim(profession_of_facilitator_psr), '')) as profession_of_facilitator_psr,
        max(nullif(trim(psychosocial_facilitator_psr), '')) as psychosocial_facilitator_psr,
        max(nullif(trim(kenyan_national_id_number_dir), '')) as kenyan_national_id_number_dir,
        max(nullif(trim(gender), '')) as gender,
        max(nullif(trim(nationality), '')) as nationality,
        max(nullif(trim(refugee_type), '')) as refugee_type,
        max(nullif(trim(county), '')) as county,
        max(nullif(trim(subcounty), '')) as subcounty,
        max(nullif(trim(ward), '')) as ward,
        max(nullif(trim(coworking_county_csr), '')) as coworking_county_csr,
        max(nullif(trim(coworking_subcounty_csr), '')) as coworking_subcounty_csr,
        max(nullif(trim(coworking_ward_csr), '')) as coworking_ward_csr,
        max(nullif(trim(primary_phone_number), '')) as primary_phone_number,
        max(nullif(trim(phone_last_8_digits), '')) as phone_last_8_digits,
        CASE
            WHEN max(CASE WHEN lower(nullif(trim(is_pwd), '')) = 'yes' THEN 1 ELSE 0 END) = 1
                THEN 'yes'
            ELSE max(nullif(trim(is_pwd), ''))
        END as is_pwd,
        max(nullif(trim(type_of_disability_dir), '')) as type_of_disability_dir,
        CASE
            WHEN max(CASE WHEN lower(nullif(trim(is_young_mother), '')) = 'yes' THEN 1 ELSE 0 END) = 1 THEN 'yes'
            WHEN max(CASE WHEN lower(nullif(trim(is_young_mother), '')) = 'don''t know' THEN 1 ELSE 0 END) = 1 THEN 'don''t know'
            ELSE max(nullif(trim(is_young_mother), ''))
        END as is_young_mother,
        max(nullif(trim(apprenticeship_provider_apr), '')) as apprenticeship_provider_apr,
        max(nullif(trim(skill_enrolled_apr), '')) as skill_enrolled_apr,
        max(placement_date_apr) as placement_date_apr,
        max(nullif(trim(grant_amount_bg), '')) as grant_amount_bg,
        max(date_grant_allocated_bg) as date_grant_allocated_bg,
        max(nullif(trim(type_of_business_you_operate_bga), '')) as type_of_business_you_operate_bga,
        max(mark_the_location_of_the_business_raw) as mark_the_location_of_the_business_raw,
        max(business_latitude) as business_latitude,
        max(business_longitude) as business_longitude,
        max(business_altitude) as business_altitude,
        max(business_accuracy) as business_accuracy,
        max(gps_of_business_location_apbl_raw) as gps_of_business_location_apbl_raw,
        max(apprenticeship_latitude) as apprenticeship_latitude,
        max(apprenticeship_longitude) as apprenticeship_longitude,
        max(apprenticeship_altitude) as apprenticeship_altitude,
        max(apprenticeship_accuracy) as apprenticeship_accuracy,
        max(location_of_institution_ttia_raw) as location_of_institution_ttia_raw,
        max(tvet_latitude) as tvet_latitude,
        max(tvet_longitude) as tvet_longitude,
        max(tvet_altitude) as tvet_altitude,
        max(tvet_accuracy) as tvet_accuracy,
        max(nullif(trim(digital_literacy_dl), '')) as digital_literacy_dl,
        max(start_date_dl) as start_date_dl,
        max(advanced_it_start_date_dl) as advanced_it_start_date_dl,
        max(advanced_it_completion_date_dl) as advanced_it_completion_date_dl,
        max(completion_date_dl) as completion_date_dl,
        max(nullif(trim(final_exams_tc), '')) as final_exams_tc,
        max(nullif(trim(dignity_package_helpful_dka), '')) as dignity_package_helpful_dka,
        max(nullif(trim(how_helpful_course), '')) as how_helpful_course,
        max(nullif(trim(how_helpful_course_tc), '')) as how_helpful_course_tc,
        max(nullif(trim(how_helpful_was_upskilling_uc), '')) as how_helpful_was_upskilling_uc,
        max(nullif(trim(why_not_helpful_tc), '')) as why_not_helpful_tc,
        max(nullif(trim(why_not_helpful_uc), '')) as why_not_helpful_uc,
        max(nullif(trim(how_helpful_course_tvet), '')) as how_helpful_course_tvet,
        max(nullif(trim(recommend_training_tvet), '')) as recommend_training_tvet,
        max(start_date_ent) as start_date_ent,
        max(completion_date_ent) as completion_date_ent,
        max(nullif(trim(interest_in_sales_work_ent), '')) as interest_in_sales_work_ent,
        max(start_date_int) as start_date_int,
        max(completion_date_int) as completion_date_int,
        max(nullif(trim(income_on_average_pl), '')) as income_on_average_pl,
        max(nullif(trim(placement_opportunity_pl), '')) as placement_opportunity_pl,
        max(date_first_visit_bm) as date_first_visit_bm,
        max(date_of_second_visit_bm) as date_of_second_visit_bm,
        max(date_of_visit_csf) as date_of_visit_csf,
        max(date_of_visit_csr) as date_of_visit_csr,
        max(nullif(trim(date_of_birth_csf), '')) as date_of_birth_csf,
        max(nullif(trim(age_in_years_csf), '')) as age_in_years_csf,
        max(nullif(trim(first_name_csf), '')) as first_name_csf,
        max(nullif(trim(second_name_csf), '')) as second_name_csf,
        max(nullif(trim(how_many_csf), '')) as how_many_csf,
        max(nullif(trim(time_in_csf), '')) as time_in_csf,
        max(nullif(trim(time_in_csr), '')) as time_in_csr,
        max(nullif(trim(time_out_csf), '')) as time_out_csf,
        max(nullif(trim(time_out_csr), '')) as time_out_csr,
        max(nullif(trim(using_coworking_for_a_living_csr), '')) as using_coworking_for_a_living_csr,
        max(nullif(trim(using_coworking_to_earn_a_living_csf), '')) as using_coworking_to_earn_a_living_csf,
        max(nullif(trim(what_visitor_is_using_csf), '')) as what_visitor_is_using_csf,
        max(nullif(trim(what_visitor_is_using_csr), '')) as what_visitor_is_using_csr,
        max(nullif(trim(purpose_for_using_csf), '')) as purpose_for_using_csf,
        max(nullif(trim(coworking_space_facility), '')) as coworking_space_facility,
        max(nullif(trim(swep_handcraft_center_hc), '')) as swep_handcraft_center_hc,
        max(start_date_hc) as start_date_hc,
        max(completion_date_hc) as completion_date_hc,
        max(nullif(trim(handcraft_training_type_hc), '')) as handcraft_training_type_hc,
        max(nullif(trim(swep_iga_center_iga), '')) as swep_iga_center_iga,
        max(start_date_iga) as start_date_iga,
        max(completion_date_iga) as completion_date_iga,
        max(nullif(trim(training_type_iga), '')) as training_type_iga,
        max(nullif(trim(swep_tailoring_center_st), '')) as swep_tailoring_center_st,
        max(start_date_of_training_st) as start_date_of_training_st,
        max(expected_end_date_of_training_st) as expected_end_date_of_training_st,
        max(nullif(trim(training_session_st), '')) as training_session_st,
        max(nullif(trim(name_of_institution_tvet), '')) as name_of_institution_tvet,
        max(nullif(trim(name_of_facilitator), '')) as name_of_facilitator,
        max(nullif(trim(course_enrolled_tvet), '')) as course_enrolled_tvet,
        max(start_date_tvet) as start_date_tvet,
        max(completion_date_tvet) as completion_date_tvet,
        CASE
            WHEN max(CASE WHEN lower(nullif(trim(nita_exams), '')) = 'yes' THEN 1 ELSE 0 END) = 1
                THEN 'yes'
            ELSE max(nullif(trim(nita_exams), ''))
        END as nita_exams
    from prepared_cases
    group by
        norm_pp_unique_id,
        norm_pp_fullname,
        norm_kenyan_id,
        phone_last_8_digits,
        norm_county
),

skill_sector_mapping as (
    select skill_enrolled_apr, skill_name, sector_name
    from (values
        -- Automotive Engineering
        ('spray_painter_automotive_engineering_sector',                          'Spray Painter',                            'Automotive Engineering'),
        ('panel_beater_automotive_engineering_sector',                          'Panel Beater',                             'Automotive Engineering'),
        ('plant_mechanic_automotive_engineering_sector',                        'Plant Mechanic',                           'Automotive Engineering'),
        ('electronic_mechanics_automotive_engineering_sector',                  'Electronic Mechanics',                     'Automotive Engineering'),
        ('motor_vehicle_mechanic_automotive_engineering_sector',                'Motor Vehicle Mechanic',                   'Automotive Engineering'),
        ('motor_vehicle_electrician_automotive_engineering_sector',             'Motor Vehicle Electrician',                'Automotive Engineering'),
        ('upholstery_automotive_engineering_sector',                            'Upholstery',                               'Automotive Engineering'),
        -- Building and Construction
        ('molder_building__construction_sector',                                'Molder',                                   'Building and Construction'),
        ('refrigeration_and_air_conditioning_building__construction_sector',    'Refrigeration and Air Conditioning',       'Building and Construction'),
        ('painter_decorator_building__construction_sector',                     'Painter Decorator',                        'Building and Construction'),
        ('cabinet_maker_building__construction_sector',                         'Cabinet Maker',                            'Building and Construction'),
        ('carpentry_and_joinery_building__construction_sector',                 'Carpentry and Joinery',                    'Building and Construction'),
        ('electrical_fitter_building__construction_sector',                     'Electrical Fitter',                        'Building and Construction'),
        ('sign_writer_building__construction_sector',                           'Sign Writer',                              'Building and Construction'),
        ('general_fitter_building__construction_sector',                        'General Fitter',                           'Building and Construction'),
        ('pipe_fitter_building__construction_sector',                           'Pipe Fitter',                              'Building and Construction'),
        ('masonry_building__construction_sector',                               'Masonry',                                  'Building and Construction'),
        ('plumber_building__construction_sector',                               'Plumber',                                  'Building and Construction'),
        ('arc_welder_building__construction_sector',                            'Arc Welder',                               'Building and Construction'),
        ('gas_welder_building__construction_sector',                            'Gas Welder',                               'Building and Construction'),
        ('woodcarver_and_wood_machinist_building__construction_sector',         'Woodcarver and Wood Machinist',             'Building and Construction'),
        ('sheet_metal_building__construction_sector',                           'Sheet Metal',                              'Building and Construction'),
        ('solar_photovoltaic_building__construction_sector',                    'Solar Photovoltaic',                       'Building and Construction'),
        ('electrical_wireman_building__construction_sector',                    'Electrical Wireman',                       'Building and Construction'),
        ('tiling_building__construction_sector',                                'Tiling',                                   'Building and Construction'),
        -- Hospitality
        ('food_and_beverage_hospitality_sector',                                'Food and Beverage',                        'Hospitality'),
        ('baking_hospitality_sector',                                           'Baking',                                   'Hospitality'),
        ('catering_hospitality_sector',                                         'Catering',                                 'Hospitality'),
        ('housekeeping_and_laundry_hospitality_sector',                         'Housekeeping and Laundry',                 'Hospitality'),
        -- Agribusiness
        ('horticulture_farm_management_agribusiness_sector',                    'Horticulture Farm Management',             'Agribusiness'),
        ('vegetable_production_and_aquaculture_agribusiness_sector',            'Vegetable Production and Aquaculture',     'Agribusiness'),
        ('dairy_farm_management_agribusiness_sector',                           'Dairy Farm Management',                    'Agribusiness'),
        -- Media
        ('photography__videography_media_sector',                               'Photography and Videography',              'Media'),
        -- Beauty Hairdressing and Textile
        ('tailoring_and_dressmaking_beautyhairdressing_and_textile_sector',     'Tailoring and Dressmaking',                'Beauty Hairdressing and Textile'),
        ('beauty_therapy_beautyhairdressing_and_textile_sector',                'Beauty Therapy',                           'Beauty Hairdressing and Textile'),
        ('hair_dressing_beautyhairdressing_and_textile_sector',                 'Hair Dressing',                            'Beauty Hairdressing and Textile'),
        -- ICT
        ('information_communication_and_technology_ict',                        'Information Communication and Technology', 'ICT'),
        -- Other
        ('other',                                                               'Other',                                    'Other')
    ) as t(skill_enrolled_apr, skill_name, sector_name)
)

select
    case_id,
    date_of_registration,
    pp_unique_id,
    pp_fullname,
    profession_of_facilitator_psr,
    psychosocial_facilitator_psr,
    gender,
    nationality,
    refugee_type,
    kenyan_national_id_number_dir,
    case
        when county is null or trim(county) = '' then null
        else
            case
                when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'kiii' then 'Kilifi'
                when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'buia' then 'Busia'
                when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'homabay' then 'Homa Bay'
                when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'kiumu' then 'Kisumu'
                when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'mombaa' then 'Mombasa'
                when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'uaingihu' then 'Uain Gihu'
                when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'trannzoia' then 'Tran Nzoia'
                when lower(regexp_replace(county, '[\\s_/-]+', '', 'g')) = 'taitataveta' then 'Taita Taveta'
                else initcap(regexp_replace(county, '[\\s_/-]+', '', 'g'))
            end
    end as county,
    case
        when subcounty is null or trim(subcounty) = '' then null
        else initcap(regexp_replace(subcounty, '[\\s_/-]+', '', 'g'))
    end as subcounty,
    case
        when ward is null or trim(ward) = '' then null
        else initcap(regexp_replace(ward, '[\\s_/-]+', '', 'g'))
    end as ward,
    coworking_county_csr,
    coworking_subcounty_csr,
    coworking_ward_csr,
    primary_phone_number,
    phone_last_8_digits,
    case when is_pwd is null or trim(is_pwd) = '' then null else initcap(is_pwd) end as is_pwd,
    nullif(trim(type_of_disability_dir), '') as type_of_disability_dir,
    case
        when is_young_mother is null or trim(is_young_mother) = '' then null
        when lower(trim(is_young_mother)) = 'yes' then 'yes'
        when lower(trim(is_young_mother)) = 'no' then 'no'
        when lower(trim(is_young_mother)) = 'don''t know' then 'don''t know'
        else lower(trim(is_young_mother))
    end as is_young_mother,
    apprenticeship_provider_apr,
    d.skill_enrolled_apr,
    placement_date_apr,
    grant_amount_bg,
    date_grant_allocated_bg,
    type_of_business_you_operate_bga,
    mark_the_location_of_the_business_raw,
    business_latitude,
    business_longitude,
    business_altitude,
    business_accuracy,
    gps_of_business_location_apbl_raw,
    apprenticeship_latitude,
    apprenticeship_longitude,
    apprenticeship_altitude,
    apprenticeship_accuracy,
    location_of_institution_ttia_raw,
    tvet_latitude,
    tvet_longitude,
    tvet_altitude,
    tvet_accuracy,
    digital_literacy_dl,
    start_date_dl,
    advanced_it_start_date_dl,
    advanced_it_completion_date_dl,
    completion_date_dl,
    final_exams_tc,
    dignity_package_helpful_dka,
    how_helpful_course,
    how_helpful_course_tc,
    how_helpful_was_upskilling_uc,
    why_not_helpful_tc,
    why_not_helpful_uc,
    how_helpful_course_tvet,
    recommend_training_tvet,
    start_date_ent,
        completion_date_ent,
        interest_in_sales_work_ent,
    start_date_int,
    completion_date_int,
    income_on_average_pl,
    placement_opportunity_pl,
    date_first_visit_bm,
    date_of_second_visit_bm,
    date_of_visit_csf,
    date_of_visit_csr,
    date_of_birth_csf,
    age_in_years_csf,
    first_name_csf,
    second_name_csf,
    how_many_csf,
    time_in_csf,
    time_in_csr,
    time_out_csf,
    time_out_csr,
    using_coworking_for_a_living_csr,
    using_coworking_to_earn_a_living_csf,
    what_visitor_is_using_csf,
    what_visitor_is_using_csr,
    purpose_for_using_csf,
    coworking_space_facility,
    swep_handcraft_center_hc,
    start_date_hc,
    completion_date_hc,
    handcraft_training_type_hc,
    swep_iga_center_iga,
    start_date_iga,
    completion_date_iga,
    training_type_iga,
    swep_tailoring_center_st,
    start_date_of_training_st,
    expected_end_date_of_training_st,
    training_session_st,
    name_of_institution_tvet,
    name_of_facilitator,
    course_enrolled_tvet,
    start_date_tvet,
    completion_date_tvet,
    nita_exams,

    m.skill_name,
    m.sector_name
from deduplicated_cases d
left join skill_sector_mapping m
    on d.skill_enrolled_apr = m.skill_enrolled_apr
