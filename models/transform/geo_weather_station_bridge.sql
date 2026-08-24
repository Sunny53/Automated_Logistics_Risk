{{ config(
    materialized='table'
) }}

-- Transform Model: geo_weather_station_bridge
-- Bridges each zip code centroid to its single closest historical weather station (synthetic grid cell).
-- It calculates geodesic distance in kilometers and flags whether the station is within our 50km QC tolerance.

with zip_centroids as (
    select * from {{ ref('geo_zip_centroid') }}
),

-- Deduplicate historical weather stations to unique (station_id, station_point) pairs.
-- Because stg_historical_weather has one row per station PER DAY, we must filter to unique pairs
-- before computing the cross-join distance matrix. Failing to do so would result in massive,
-- highly inflated Cartesian products and duplicate nearest-neighbor rows.
distinct_stations as (
    select distinct
        station_id,
        station_point
    from {{ ref('stg_historical_weather') }}
),

-- Compute the geodesic distance between every zip centroid and every distinct weather station.
-- In Snowflake, ST_DISTANCE returns distance in meters. We divide by 1000.0 to convert to kilometers.
distances_calc as (
    select
        z.zip_code_prefix,
        s.station_id,
        st_distance(z.centroid_point, s.station_point) / 1000.0 as distance_km
    from zip_centroids z
    cross join distinct_stations s
)

-- Select the single closest weather station per zip prefix using a QUALIFY clause.
-- QUALIFY allows us to filter the results of our ROW_NUMBER() window function without a subquery.
select
    zip_code_prefix,
    station_id,
    distance_km,
    -- If the closest station is within our 50 km tolerance, set to true, else false.
    (distance_km <= 50.0) as is_within_tolerance
from distances_calc
-- Secondary deterministic tiebreaker on station_id ensures reproducible results when distances are equal.
qualify row_number() over (
    partition by zip_code_prefix
    order by distance_km asc, station_id asc
) = 1
