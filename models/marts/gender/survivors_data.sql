with
    survivors_data as (
        select
            assigned_to,
            case_id,
            case
                when date_of_birth is null
                then
                    case
                        when what_is_the_age_provided is not null and what_is_the_age_provided != ''
                        then nullif(what_is_the_age_provided, '')::int
                        else
                            case
                                when
                                    what_is_the_year_of_birth_of_survivor is not null
                                    and length(what_is_the_year_of_birth_of_survivor) = 4
                                    and nullif(what_is_the_year_of_birth_of_survivor, '')::int
                                    between 1900 and extract(year from current_date)
                                then
                                    extract(
                                        year
                                        from
                                            age(
                                                to_date(
                                                    what_is_the_year_of_birth_of_survivor,
                                                    'YYYY'
                                                )
                                            )
                                    )::int
                                else null
                            end
                    end
                else
                    case
                        -- Check if the text can be safely converted to a date and is
                        -- within a valid range
                        when
                            to_date(date_of_birth, 'YYYY-MM-DD') >= '1900-01-01'::date
                            and to_date(date_of_birth, 'YYYY-MM-DD') <= current_date
                        then
                            extract(
                                year
                                from
                                    age(
                                        current_date,
                                        to_date(date_of_birth, 'YYYY-MM-DD')
                                    )
                            )::int
                        else null
                    end
            end as age,

            {{ validate_date("date_of_registration") }} as date_of_registration,
            gender,
            gender_site_code_of_registration,
            county as county_code,
            constituency as constituency_code,
            village as village_name,
            ward as ward_code,
            case_type,
            {{ validate_date("date_opened") }} as date_opened,
            indexed_on,
            closed,
            {{ validate_date("created_at") }} as created_at
        from {{ ref("staging_gender_survivors_commcare") }}
    )

select
    a.*,
    coalesce(b.county_name, a.county_code) as county_name,
    coalesce(b.constituency_name, a.constituency_code) as constituency_name,
    coalesce(b.ward_name, a.ward_code) as ward_name,
    c.site_name as gender_site_name_of_registration
from survivors_data a
left join
    {{ source("source_commcare", "dim_location_administrative_units") }} b
    on a.county_code = b.county_code
    and a.constituency_code = b.constituency_id
    and a.ward_code = b.ward_id
left join
    {{ source("source_commcare", "dim_gender_sites") }} c
    on a.gender_site_code_of_registration = c.site_code