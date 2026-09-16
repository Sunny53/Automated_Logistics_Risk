{{ config(materialized='table') }}

-- Fact Model: fact_delivery_weather
-- Grain: one row per order (order_id)
-- Purpose: Joins orders with destination and origin weather data to analyze
--          the relationship between weather conditions and delivery performance.
--
-- Known Limitations:
-- 1. Weather ingestion is still in progress as of this build. Orders whose
--    relevant zip prefix's nearest weather station has no weather data yet
--    for the needed date range will show NULL weather aggregates, not zero
--    or an error. This is expected and will resolve as ingestion completes.
-- 2. For orders with multiple sellers (multiple order_items), this model
--    uses only the first seller's origin weather (selected by minimum
--    order_item_id). This simplification keeps the grain at one row per
--    order but means multi-seller orders do not reflect all origin locations.
--    This is a known simplification, not a bug.

with orders as (
    select * from {{ ref('stg_olist_orders') }}
),

-- Get first seller per order (by order_item_id) to maintain one-row-per-order grain
order_items_ranked as (
    select
        order_id,
        seller_id,
        order_item_id,
        row_number() over (
            partition by order_id
            order by order_item_id asc
        ) as seller_rn
    from {{ ref('stg_olist_order_items') }}
),

first_seller_per_order as (
    select
        order_id,
        seller_id
    from order_items_ranked
    where seller_rn = 1
),

customers as (
    select * from {{ ref('stg_olist_customers') }}
),

sellers as (
    select * from {{ ref('stg_olist_sellers') }}
),

-- Bridge destination zip to weather station
dest_weather_stations as (
    select * from {{ ref('geo_weather_station_bridge') }}
),

-- Bridge origin zip to weather station (same model, different alias context)
origin_weather_stations as (
    select * from {{ ref('geo_weather_station_bridge') }}
),

-- Source weather data for destination aggregations
dest_weather as (
    select * from {{ ref('stg_historical_weather') }}
),

-- Source weather data for origin aggregations
origin_weather as (
    select * from {{ ref('stg_historical_weather') }}
),

-- Join all elements together, with destination and origin weather aggregations
fact_with_destination_weather as (
    select
        o.order_id,
        o.customer_id,
        fso.seller_id,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        o.is_late_delivery,
        dws.station_id as destination_station_id,
        dws.distance_km as destination_distance_km,
        dws.is_within_tolerance as destination_is_within_tolerance,
        max(dw.precipitation_mm) as destination_max_precipitation_mm,
        max(dw.wind_speed_kmh) as destination_max_wind_speed_kmh,
        boolor_agg(dw.severe_weather_flag) as destination_had_severe_weather
    from orders o
    left join first_seller_per_order fso on o.order_id = fso.order_id
    left join customers c on o.customer_id = c.customer_id
    left join dest_weather_stations dws
        on c.customer_zip_code_prefix = dws.zip_code_prefix
    left join dest_weather dw
        on dws.station_id = dw.station_id
        and dw.weather_date between
            o.order_purchase_timestamp::date
            and coalesce(o.order_delivered_customer_date::date, o.order_purchase_timestamp::date)
    group by
        o.order_id,
        o.customer_id,
        fso.seller_id,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        o.is_late_delivery,
        dws.station_id,
        dws.distance_km,
        dws.is_within_tolerance
),

-- Add origin weather aggregations
fact_with_all_weather as (
    select
        fwd.order_id,
        fwd.customer_id,
        fwd.seller_id,
        fwd.order_purchase_timestamp,
        fwd.order_delivered_customer_date,
        fwd.order_estimated_delivery_date,
        fwd.is_late_delivery,
        fwd.destination_station_id,
        fwd.destination_distance_km,
        fwd.destination_is_within_tolerance,
        fwd.destination_max_precipitation_mm,
        fwd.destination_max_wind_speed_kmh,
        fwd.destination_had_severe_weather,
        ows.station_id as origin_station_id,
        ows.distance_km as origin_distance_km,
        ows.is_within_tolerance as origin_is_within_tolerance,
        max(ow.precipitation_mm) as origin_max_precipitation_mm,
        max(ow.wind_speed_kmh) as origin_max_wind_speed_kmh,
        boolor_agg(ow.severe_weather_flag) as origin_had_severe_weather
    from fact_with_destination_weather fwd
    left join sellers s on fwd.seller_id = s.seller_id
    left join origin_weather_stations ows
        on s.seller_zip_code_prefix = ows.zip_code_prefix
    left join origin_weather ow
        on ows.station_id = ow.station_id
        and ow.weather_date between
            fwd.order_purchase_timestamp::date
            and coalesce(fwd.order_delivered_customer_date::date, fwd.order_purchase_timestamp::date)
    group by
        fwd.order_id,
        fwd.customer_id,
        fwd.seller_id,
        fwd.order_purchase_timestamp,
        fwd.order_delivered_customer_date,
        fwd.order_estimated_delivery_date,
        fwd.is_late_delivery,
        fwd.destination_station_id,
        fwd.destination_distance_km,
        fwd.destination_is_within_tolerance,
        fwd.destination_max_precipitation_mm,
        fwd.destination_max_wind_speed_kmh,
        fwd.destination_had_severe_weather,
        ows.station_id,
        ows.distance_km,
        ows.is_within_tolerance
)

select
    order_id,
    customer_id,
    seller_id,
    order_purchase_timestamp,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    is_late_delivery,
    destination_station_id,
    destination_distance_km,
    destination_is_within_tolerance,
    destination_max_precipitation_mm,
    destination_max_wind_speed_kmh,
    destination_had_severe_weather,
    origin_station_id,
    origin_distance_km,
    origin_is_within_tolerance,
    origin_max_precipitation_mm,
    origin_max_wind_speed_kmh,
    origin_had_severe_weather
from fact_with_all_weather
