{{ 
    config(
        materialized='incremental',
        unique_key='case_id',
        alias='sl_new_analytics',
        tags=['analytics', 'sustainable_livelihoods', 'sl_new']
    ) 
}}

with source as (

    select * 
    from {{ ref('staging_sl_new') }}

),

renamed as (

    select 
        case_id,
        pp_fullname,
        pp_sex,
        nullif(pp_age, '')::integer as age,
        contact_phone_number,

        -- Safe casting of date_of_birth
        case 
            when length(date_of_birth) = 10 and date_of_birth ~ '^\d{4}-\d{2}-\d{2}$' 
                 and substring(date_of_birth, 1, 4)::int <= 2100
            then date_of_birth::date
            else null 
        end as date_of_birth,

        -- Safe casting of date_of_registration
        case 
            when length(date_of_registration) = 10 and date_of_registration ~ '^\d{4}-\d{2}-\d{2}$' 
                 and substring(date_of_registration, 1, 4)::int <= 2100
            then date_of_registration::date
            else null 
        end as date_of_registration,

        course_enrolled,
        employment_type,
        participants_program,
        willingness_to_relocate,
        prior_skills,
        training_activity,
        completed_training,
        confident_to_find_employment,
        entrepreneur_support,
        preferred_employment_sector,
        job_placement_support,
        post_training_support,
        formal_education,
        education_level,
        county,
        constituency,
        ward,
        unique_id,
        is_pwd,
        enumerator,
        pp_shofco_county,
        pp_shofco_subcounty,
        pp_ahofco_ward,
        sex_dir,
        sex_uid,
        common_training,
        unique_training,
        computer_literate,
        interest_in_sales_work,
        participants_type_of_work,
        status_of_education,
        primary_phone_number,
        alternative_phone_number,
        national_id_number,
        is_existing_participant,
        specify_shofco_program,
        name_consent,

        -- Derived fields
        case 
            when nullif(pp_age, '')::integer < 18 then 'Under 18'
            when nullif(pp_age, '')::integer between 18 and 24 then '18-24'
            when nullif(pp_age, '')::integer between 25 and 35 then '25-35'
            when nullif(pp_age, '')::integer > 35 then '35+'
            else 'Unknown'
        end as age_group,

        case 
            when completed_training = 'yes' then 'Completed'
            when completed_training = 'no' then 'Not Completed'
            else 'Unknown'
        end as training_completion_status,

        case 
            when computer_literate = 'yes' then 'Computer Literate'
            when computer_literate = 'no' then 'Not Computer Literate'
            else 'Unknown'
        end as computer_literacy_status

    from source
)

select * from renamed
