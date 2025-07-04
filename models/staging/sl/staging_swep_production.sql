{{
    config(
        materialized='table',
        tags=['swep', 'swep_production', 'sl']
    )
}}

with source_data as (
    select
        id,
        data
    from {{ source('staging_sl', 'SWEP_Production_Details') }}
    WHERE
        data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
),

base as (
    select
        id as form_id,
        ((data::jsonb) -> 'form' -> 'case' ->> '@case_id') as case_id,
        NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'date_of_production_sp', '') as date_of_production_sp,
        NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'unique_items_produced_sp', '') as unique_items_produced_sp,
        jsonb_path_query(data::jsonb, '$.form.product_made_sp[*]') as product_item
    from source_data
),

exploded as (
    select
        form_id,
        case_id,
        date_of_production_sp,
        unique_items_produced_sp,
        product_item::jsonb -> 'details_of_products_made_sp' ->> 'total_cost_sp' as total_cost_sp,
        product_item::jsonb -> 'details_of_products_made_sp' ->> 'fill_instructions_sp' as fill_instructions_sp,
        product_item::jsonb -> 'details_of_products_made_sp' ->> 'product_made_sp' as product_name,
        product_item::jsonb -> 'details_of_products_made_sp' ->> 'revenue_sp' as revenue_sp,
        product_item::jsonb -> 'details_of_products_made_sp' ->> 'pieces_produced_sp' as pieces_produced_sp,
        product_item::jsonb -> 'details_of_products_made_sp' ->> 'amount_per_piece_sp' as amount_per_piece_sp
    from base
)

select * from exploded