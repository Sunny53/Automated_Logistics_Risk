{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_logistics', 'raw_olist_order_items') }}
),

renamed as (
    select
        order_id,
        order_item_id::int          as order_item_id,
        product_id,
        seller_id,
        shipping_limit_date::timestamp as shipping_limit_date,
        price::float                 as price,
        freight_value::float         as freight_value,
        current_timestamp() as _loaded_at
    from source
)

select * from renamed
