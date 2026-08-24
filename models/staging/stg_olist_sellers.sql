{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_logistics', 'raw_olist_sellers') }}
),

renamed as (
    select
        seller_id,
        seller_zip_code_prefix::string as seller_zip_code_prefix,
        seller_city,
        seller_state,
        current_timestamp() as _loaded_at
    from source
)

select * from renamed
