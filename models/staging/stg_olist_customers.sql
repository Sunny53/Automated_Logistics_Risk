{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_logistics', 'raw_olist_customers') }}
),

renamed as (
    select
        customer_id,
        customer_zip_code_prefix::string as customer_zip_code_prefix,
        customer_city,
        customer_state,
        current_timestamp() as _loaded_at
    from source
)

select * from renamed
