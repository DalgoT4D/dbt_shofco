{{
    config(
        materialized='table',
        tags=['staging', 'sl_staging', 'sl', 'sl_mel'],
        post_hook="
            update {{ this }}
            set evaluation_date = date '1899-12-30' + floor(trim(evaluation_date_raw)::numeric)::integer
            where evaluation_date is null
              and trim(coalesce(evaluation_date_raw, '')) ~ '^[0-9]{5}(\\.[0-9]+)?$'
        "
    )
}}

{% set raw_columns = [
    ('Evaluation Program', 'evaluation_program'),
    ('Respondent Name', 'respondent_name'),
    ('Gender', 'gender'),
    ('Primary Telephone', 'primary_telephone'),
    ('Alternative  telephone', 'alternative_telephone'),
    ('YOB', 'yob'),
    ('Age', 'age'),
    ('Nationality (Kenyan/Refugees/Non-Kenyans)', 'nationality'),
    ('National ID Number', 'national_id_number'),
    ('P.O.R', 'por'),
    ('Education', 'education'),
    ('County', 'county'),
    ('Sub-county', 'subcounty'),
    ('ward', 'ward'),
    ('With disability', 'with_disability'),
    ('Disability type', 'disability_type'),
    ('Other disability type', 'other_disability_type'),
    ('Developmental disability dissability_type', 'developmental_disability_disability_type'),
    ('With disability certificate (Yes/No)', 'with_disability_certificate'),
    ('Difficulty area', 'difficulty_area'),
    ('Difficulty in selfcare', 'difficulty_in_selfcare'),
    ('Difficulty in memory', 'difficulty_in_memory'),
    ('Difficulty in speaking', 'difficulty_in_speaking'),
    ('Difficulty in walking', 'difficulty_in_walking'),
    ('Difficulty in hearing', 'difficulty_in_hearing'),
    ('Difficulty in seeing', 'difficulty_in_seeing'),
    ('Parental status (Yes/No)', 'parental_status'),
    ('Number of children', 'number_of_children'),
    ('children 0-15 yrs', 'children_0_15_years'),
    ('children 16-35 yrs', 'children_16_35_years'),
    ('HH with child (4-18 yrs) not in sch.', 'household_with_child_4_18_not_in_school'),
    ('Number of children not in school', 'number_of_children_not_in_school'),
    ('Main reason why children are not in school', 'main_reason_children_not_in_school'),
    ('Are you a SUN member?', 'is_sun_member'),
    ('Did you train with SHOFCO', 'trained_with_shofco'),
    ('Program enrolled', 'program_enrolled_raw'),
    ('Participation Type', 'participation_type'),
    ('If SWEP, indicate SWEP course taken (e.g. Tailoring, Beadwork, ', 'swep_course_taken'),
    ('Specific skills acquired', 'specific_skills_acquired'),
    ('Training duration', 'training_duration'),
    ('Training centre/institution attended', 'training_centre_institution_attended'),
    ('Did you complete the program you were enrolled in? (Yes/No)', 'completed_enrolled_program'),
    ('Exam type e.g NITA', 'exam_type'),
    ('SATISFACTION LEVEL: Training content', 'training_content_satisfaction'),
    ('SATISFACTION LEVEL: Training Delivery', 'training_delivery_satisfaction'),
    ('OVERAL Training satisfaction', 'overall_training_satisfaction'),
    ('Do you feel you have the skills for work/job ready', 'feels_job_ready'),
    ('Currently applying training skills acquired (e.g. in work, proj', 'currently_applying_training_skills'),
    ('Program improvement suggestions/Recommendations', 'program_improvement_suggestions'),
    ('Were you trained before SHOFCO support/did you have any skills/', 'had_skills_before_shofco_support'),
    ('What skills did you have before SHOFCO?', 'pre_shofco_skills'),
    ('Employment Status before shofco', 'employment_status_before_shofco'),
    ('Were you running a business before the SHOFCO support?', 'ran_business_before_shofco_support'),
    ('Business type before SHOFCO support', 'business_type_before_shofco_support'),
    ('Number of business branches before SHOFCO support', 'business_branches_before_shofco_support'),
    ('Business ownership status before SHOFCO support', 'business_ownership_status_before_shofco_support'),
    ('Number of emloyees before SHOFCO support', 'employees_before_shofco_support'),
    ('Was this business registered?', 'business_registered_before_shofco_support'),
    ('Total montly income before SHOCO support', 'total_monthly_income_before_shoco_support_raw'),
    ('Montly SALES before SHOFCO support', 'monthly_sales_before_shofco_support_raw'),
    ('Monthly PROFITS before SHOFCO support', 'monthly_profits_before_shofco_support_raw'),
    ('CURRENT Employment status', 'current_employment_status'),
    ('CURRENT Total Montly Income', 'current_total_monthly_income_raw'),
    ('Are youn CUURENTLY running a business? (Yes/No)', 'currently_running_business'),
    ('CURENT Business type', 'current_business_type'),
    ('Number of business branches CURRENTLY', 'current_business_branches'),
    ('CURRENT Business ownership status', 'current_business_ownership_status'),
    ('Is this business registered?', 'current_business_registered'),
    ('Have you created employment for others/engaged other people to ', 'created_employment_after_shofco_support'),
    ('Number of people employed/engaged AFTER SHOFCO support', 'people_employed_after_shofco_support'),
    ('Number of employees 35 yrs and below (18-35)', 'employees_18_35_after_shofco_support'),
    ('Number of FEMALE employees 35 yrs and below (18-35)', 'female_employees_18_35_after_shofco_support'),
    ('Number of MALE employees 35 yrs and below (18-35)', 'male_employees_18_35_after_shofco_support'),
    ('CURRENT Total Montly INCOME', 'current_business_total_monthly_income_raw'),
    ('CURRENT Total Monthly SALES', 'current_total_monthly_sales_raw'),
    ('CURRENT Total monthly PROFITS', 'current_total_monthly_profits_raw'),
    ('JOB SATISFACTION: Current work environment rating', 'job_satisfaction_current_work_environment_rating'),
    ('JOB SATISFACTION: Current work-life balance rating', 'job_satisfaction_current_work_life_balance_rating'),
    ('JOB SATISFACTION: My income is enoug for my dependents', 'job_satisfaction_income_enough_for_dependents'),
    ('JOB SATISFACTION: My work is respected byothers', 'job_satisfaction_work_respected_by_others'),
    ('JOB SATISFACTION: I feel respected at work', 'job_satisfaction_feel_respected_at_work'),
    ('JOB SATISFACTION: My work gives me a sense of purpose', 'job_satisfaction_work_gives_sense_of_purpose'),
    ('Experienced discrimination, abuse, exploitation, neglect, or ha', 'experienced_discrimination_abuse_exploitation_neglect_harassment'),
    ('UNEMPLOYED/VOLUNTEER: What barriers do you currently face in yo', 'unemployed_volunteer_current_barriers'),
    ('UNEMPLOYED/VOLUNTEER: Which of the following best describes you', 'unemployed_volunteer_status'),
    ('UNEMPLOYED/VOLUNTEER: What major support would you need the mos', 'unemployed_volunteer_major_support_needed'),
    ('UNEMPLOYED/VOLUNTEER: Which sectors would you be interested in?', 'unemployed_volunteer_sectors_of_interest'),
    ('UNEMPLOYED/VOLUNTEER: Would you be willing to relocate or work ', 'willing_to_relocate_or_work'),
    ('Description of what happened', 'incident_description'),
    ('did you have a child under 4 yrs when you joined SHOFCO program', 'had_child_under_4_when_joined_shofco'),
    ('Did you receive DAYCARE support', 'received_daycare_support'),
    ('Did the daycare support help you to balance between childcare a', 'daycare_support_helped_balance_childcare'),
    ('Did the availability of daycare support influence your decision', 'daycare_support_influenced_decision'),
    ('What are some of the gaps in the Daycare program', 'daycare_program_gaps'),
    ('Did you receive dignity kits?', 'received_dignity_kits'),
    ('How dignity kits impacted your attendance in the program', 'dignity_kits_impact_on_attendance'),
    ('Satisfaction level with Dignity Kit contents', 'dignity_kit_contents_satisfaction'),
    ('Dit the dignity kit improve your confidence to participate in S', 'dignity_kit_improved_confidence_to_participate'),
    ('Dignity kit program improvement areas (recommensations/suggesti', 'dignity_kit_program_improvement_areas'),
    ('In the last 3 months, has your household been able to get the h', 'household_able_to_get_healthcare_last_3_months'),
    ('Do you currently have any debts for any of the following?', 'current_debt_types'),
    ('Where do you save money', 'savings_location'),
    ('Predominant wall_material', 'predominant_wall_material'),
    ('cooking_energy type', 'cooking_energy_type'),
    ('consumed Rice in the past 7 days', 'consumed_rice_past_7_days'),
    ('consumed Wheat in the past 7 days', 'consumed_wheat_past_7_days'),
    ('consumed Bread in the past 7 days', 'consumed_bread_past_7_days'),
    ('consumed Beef in the past 7 days', 'consumed_beef_past_7_days'),
    ('consumed Chicken in the past 7 days', 'consumed_chicken_past_7_days'),
    ('consumed Eggs in the past 7 days', 'consumed_eggs_past_7_days'),
    ('consumed Margarine in the past 7 days', 'consumed_margarine_past_7_days'),
    ('Education level of the Female HH hed', 'female_household_head_education_level'),
    ('Highest education level achieved in the HH', 'household_highest_education_level'),
    ('Have you consumed/acquired Bread, meat, fish, or banana in the ', 'consumed_bread_meat_fish_or_banana_past_7_days'),
    ('Have you consumed/acquired Bread in the past 7 days?', 'consumed_bread_acquired_past_7_days'),
    ('Have you consumed/acquired Meat or Fish in the past 7 days?', 'consumed_meat_or_fish_acquired_past_7_days'),
    ('Have you consumed/acquired Bananas in the past 7 days?', 'consumed_bananas_acquired_past_7_days'),
    ('Does your HH own towel', 'owns_towel'),
    ('Does your HH own thermos', 'owns_thermos'),
    ('Predominant wall material of the main dwelling unit', 'main_dwelling_unit_wall_material'),
    ('Predominant floor material of the main dwelling unit', 'main_dwelling_unit_floor_material'),
    ('House type', 'house_type')
] %}

select
    "Evaluation Date" as evaluation_date_raw,
    case
        when trim(coalesce("Evaluation Date", '')) ~ '^[0-9]{5}(\\.[0-9]+)?$'
            then date '1899-12-30' + floor(trim("Evaluation Date")::numeric)::integer
        else {{ clean_sheet_date('"Evaluation Date"') }}
    end as evaluation_date,
    "Date of Birth (YoB)" as date_of_birth_raw,
    case
        when trim(coalesce("Date of Birth (YoB)", '')) ~ '^[0-9]{5}(\\.[0-9]+)?$'
            then date '1899-12-30' + floor(trim("Date of Birth (YoB)")::numeric)::integer
        else {{ clean_sheet_date('"Date of Birth (YoB)"') }}
    end as date_of_birth,
    "Date enrolled" as date_enrolled_raw,
    case
        when trim(coalesce("Date enrolled", '')) ~ '^[0-9]{5}(\\.[0-9]+)?$'
            then date '1899-12-30' + floor(trim("Date enrolled")::numeric)::integer
        else {{ clean_sheet_date('"Date enrolled"') }}
    end as date_enrolled,
{% for source_name, alias in raw_columns %}
    "{{ source_name }}" as {{ alias }}{% if not loop.last %},{% endif %}
{% endfor %}
from {{ source('staging_sl', 'sl_mel_database') }}
