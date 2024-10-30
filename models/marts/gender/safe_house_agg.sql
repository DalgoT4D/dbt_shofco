with
    -- Fetch the case data with onboarding date
    case_occurrences_data as (
        select
            case_id,
            {{ validate_date("date_of_safehouse_onboarding") }} as date_of_safe_house_onboarding
        from {{ ref("staging_gender_safe_house_commcare") }}
        where {{ validate_date("date_of_safehouse_onboarding") }} IS NOT NULL
    )

-- Final query: count the number of people onboarded to the safe house per month
select
    date_trunc('month', date_of_safe_house_onboarding) as month,
    count(distinct case_id) as people_onboarded
from case_occurrences_data
group by month
order by month