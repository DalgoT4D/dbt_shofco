{{ config(
  materialized='table',
  tags=['business_grants_assessment', 'sl']
) }}

WITH assessment_data AS (
    SELECT
        id,
        
        -- Case metadata
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'Enumerator' AS enumerator,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'intern_name' AS intern_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'Enumerator_First_Name' AS enumerator_first_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'Enumerator_Second_Name' AS enumerator_second_name,

        -- Location & business validation section
        data::jsonb -> 'form' -> 'validation_of_key_business_details' -> 'if_existing_bga' ->> 'ward_business_bga' AS ward,
        data::jsonb -> 'form' -> 'validation_of_key_business_details' -> 'if_existing_bga' ->> 'subcounty_business_bga' AS subcounty,
        data::jsonb -> 'form' -> 'validation_of_key_business_details' -> 'if_existing_bga' ->> 'county_business_bga' AS county,
        data::jsonb -> 'form' -> 'validation_of_key_business_details' -> 'if_existing_bga' ->> 'type_of_business_you_operate_bga' AS type_of_business,
        data::jsonb -> 'form' -> 'validation_of_key_business_details' -> 'if_existing_bga' ->> 'shofco_training_participated_in_bga' AS shofco_training,
        data::jsonb -> 'form' -> 'validation_of_key_business_details' -> 'if_existing_bga' ->> 'is_business_registered_with_govt_bga' AS is_registered,
        data::jsonb -> 'form' -> 'validation_of_key_business_details' -> 'if_existing_bga' ->> 'how_long_business_been_in_operation_bga' AS business_duration,
        data::jsonb -> 'form' -> 'validation_of_key_business_details' -> 'if_existing_bga' ->> 'how_much_money_required_to_implement_bga' AS amount_required,
        data::jsonb -> 'form' -> 'validation_of_key_business_details' -> 'if_existing_bga' ->> 'does_business_have_employees' AS has_employees,
        data::jsonb -> 'form' -> 'validation_of_key_business_details' -> 'if_existing_bga' ->> 'benifited_from_shofco_bga' AS benefited_from_shofco,
        data::jsonb -> 'form' -> 'validation_of_key_business_details' ->> 'Category_of_Business_bga' AS business_category,
        data::jsonb -> 'form' -> 'validation_of_key_business_details' ->> 'Nature_of_your_business_bga' AS business_nature,

        -- Assessor Technical Review
        data::jsonb -> 'form' -> 'ASSESSORS_TECHNICAL_REVIEW' -> 'business_profile' ->> 'business_potential_bga' AS business_potential,
        data::jsonb -> 'form' -> 'ASSESSORS_TECHNICAL_REVIEW' -> 'business_profile' ->> 'on_rate_is_business_idea_viable_bga' AS viability_rating,
        data::jsonb -> 'form' -> 'ASSESSORS_TECHNICAL_REVIEW' -> 'business_profile' ->> 'on_rate_ismarket_identified_realistic_bga' AS market_realism_rating,
        data::jsonb -> 'form' -> 'ASSESSORS_TECHNICAL_REVIEW' -> 'management-implmentation' ->> 'does_business_have_capacity_for_growth_bga' AS has_growth_capacity,
        data::jsonb -> 'form' -> 'ASSESSORS_TECHNICAL_REVIEW' -> 'experience_and_business_performance' ->> 'on_rate_does_applicantgroup_have_experience_bga' AS experience_rating,
        data::jsonb -> 'form' -> 'ASSESSORS_TECHNICAL_REVIEW' -> 'experience_and_business_performance' ->> 'on_rate_does_applicantgroup_have_ability_to_leverage_finances_bga' AS leverage_rating,
        data::jsonb -> 'form' -> 'ASSESSORS_TECHNICAL_REVIEW' ->> 'yes_provide_details_bga' AS business_potential_details,
        data::jsonb -> 'form' -> 'ASSESSORS_TECHNICAL_REVIEW' ->> 'business_applicant_eligible_for_grant_bga' AS is_eligible,

        -- Additional Details Required
        data::jsonb -> 'form' -> 'additional_details_required' ->> 'Purpose_of_the_grant_bga' AS grant_purpose,
        data::jsonb -> 'form' -> 'additional_details_required' ->> 'Value_of_available_stock_bga' AS stock_value,
        data::jsonb -> 'form' -> 'additional_details_required' ->> 'willing_to_mentor_others_bga' AS willing_to_mentor,
        data::jsonb -> 'form' -> 'additional_details_required' ->> 'how_to_mitigate_challenges_bga' AS challenge_mitigation,
        data::jsonb -> 'form' -> 'additional_details_required' ->> 'key_challenges_that_might_affect_the_business_bga' AS key_challenges,
        data::jsonb -> 'form' -> 'additional_details_required' ->> 'reasons_you_think_your_business_should_be_selected_bga' AS reason_for_selection,

        -- Submission metadata
        data::jsonb -> 'form' -> 'meta' ->> 'username' AS submitted_by,
        data::jsonb -> 'form' -> 'meta' ->> 'timeStart' AS time_start,
        data::jsonb -> 'form' -> 'meta' ->> 'timeEnd' AS time_end,
        data::jsonb ->> 'received_on' AS date_received

    FROM {{ source('staging_sl', 'Business_Grants_Assessment___Tracking_Form') }}
    WHERE data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
)

SELECT DISTINCT *
FROM assessment_data
