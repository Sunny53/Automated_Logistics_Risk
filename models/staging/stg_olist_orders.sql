{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_logistics', 'raw_olist_orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp::timestamp        as order_purchase_timestamp,
        order_approved_at::timestamp                as order_approved_at,
        order_delivered_carrier_date::timestamp     as order_delivered_carrier_date,
        order_delivered_customer_date::timestamp    as order_delivered_customer_date,
        order_estimated_delivery_date::timestamp    as order_estimated_delivery_date,
        case
            when order_delivered_customer_date::timestamp
                 > order_estimated_delivery_date::timestamp
            then true
            else false
        end as is_late_delivery,
        current_timestamp() as _loaded_at
    from source
)

select * from renamed
