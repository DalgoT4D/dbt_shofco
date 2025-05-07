{{ 
    config(
        materialized='table',
        schema='sl',
        alias='sl_coworking_space_user_engagement',
        tags=['sl', 'coworking_space', 'joined', 'user_engagement']
    ) 
}}

WITH registration AS (
    SELECT *
    FROM {{ ref('staging_sl_coworking_space_registration') }}
),

visits AS (
    SELECT *
    FROM {{ ref('staging_sl_coworking_space_visit') }}
)

SELECT
    r.case_id AS registration_case_id,
    r.owner_id,
    r.external_id,
    r.gender,
    r.is_plwd,
    r.id_number,
    r.first_name,
    r.middle_name,
    r.last_name_surname,
    r.date_of_birth,
    r.nationality,
    r.officer_name AS registration_officer_name,
    r.primary_phone_number,
    r.alternative_phone_number,

    v.case_id AS visit_case_id,
    v.date_of_visit,
    v.time_in,
    v.time_out,
    v.purpose_of_visit,
    v.specific_reason_for_accessing_space,
    v.ward_shofco_site,
    v.subcounty_shofco_site,
    v.county_shofco_site,
    v.officer_name AS visit_officer_name

FROM visits v
LEFT JOIN registration r
    ON v.owner_id = r.owner_id
