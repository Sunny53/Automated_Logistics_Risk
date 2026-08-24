{{ config(materialized='view') }}

-- Staging: ERA5 historical weather grid data (Open-Meteo Historical Weather API)
-- Note: station_id is a synthetic grid-cell identifier, NOT a physical weather
-- station. ERA5 is reanalysis data (model + observation blend), not raw
-- thermometer readings — this is intentional (see Part 1 discussion) and
-- documented here so nothing downstream mistakes it for station telemetry.

with source as (
    select * from {{ source('raw_logistics', 'raw_historical_weather_ingest') }}
),

renamed as (
    select
        md5(concat(hub_identifier, '_', logistics_date::string))  as staging_weather_id,
        trim(hub_identifier)                                       as station_id,
        latitude::float                                            as station_lat,
        longitude::float                                           as station_lng,
        st_makepoint(longitude::float, latitude::float)            as station_point,
        logistics_date::date                                       as weather_date,
        temp_max_celsius::float                                    as max_temp_c,
        temp_min_celsius::float                                    as min_temp_c,
        (temp_max_celsius::float + temp_min_celsius::float) / 2.0  as avg_temp_c,
        precipitation_sum_mm::float                                as precipitation_mm,
        wind_speed_max_kmh::float                                  as wind_speed_kmh,
        -- single staging-layer flag only; severity BUCKETING belongs in the
        -- transform/mart layer, not here
        case
            when precipitation_sum_mm::float > 20.0
              or wind_speed_max_kmh::float > 40.0
            then true
            else false
        end as severe_weather_flag,
        current_timestamp() as _loaded_at
    from source
)

select * from renamed
