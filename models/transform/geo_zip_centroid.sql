{{ config(
    materialized='table'
) }}

-- Transform Model: geo_zip_centroid
-- Computes the geographic center (median latitude and longitude) for each zip_code_prefix
-- using crowdsourced coordinates. Median is preferred over average because it is robust
-- against GPS ping outliers and typos. It also calculates the Mode state for each zip prefix.

with raw_geolocation as (
    select * from {{ ref('stg_olist_geolocation') }}
),

-- 1. Compute geographic median latitude and longitude using Snowflake's APPROX_PERCENTILE (median),
-- and count the number of pings behind each prefix.
centroid_coords as (
    select
        geolocation_zip_code_prefix as zip_code_prefix,
        approx_percentile(geolocation_lat, 0.5) as centroid_lat,
        approx_percentile(geolocation_lng, 0.5) as centroid_lng,
        count(*) as ping_count
    from raw_geolocation
    group by 1
),

-- 2. Determine the mode (most frequent) of geolocation_state per prefix.
state_counts as (
    select
        geolocation_zip_code_prefix as zip_code_prefix,
        geolocation_state as state,
        count(*) as occurrences
    from raw_geolocation
    where geolocation_state is not null
    group by 1, 2
),

state_ranked as (
    select
        zip_code_prefix,
        state,
        -- Rank states by occurrences. If there is a tie, we break it deterministically
        -- by sorting alphabetically by state name.
        row_number() over (
            partition by zip_code_prefix
            order by occurrences desc, state asc
        ) as state_rank
    from state_counts
),

state_mode as (
    select
        zip_code_prefix,
        state
    from state_ranked
    where state_rank = 1
)

-- 3. Construct the final table with coordinates, Snowflake GEOGRAPHY point, ping counts, and state mode.
select
    c.zip_code_prefix,
    c.centroid_lat,
    c.centroid_lng,
    -- Constructing the GEOGRAPHY object using Snowflake's ST_MAKEPOINT(lng, lat).
    -- Longitude MUST be the first argument, and latitude the second argument.
    st_makepoint(c.centroid_lng, c.centroid_lat) as centroid_point,
    c.ping_count,
    -- Prefixes with no non-null state pings default to 'UNKNOWN' to prevent failing the not_null schema test.
    coalesce(s.state, 'UNKNOWN') as state
from centroid_coords c
left join state_mode s on c.zip_code_prefix = s.zip_code_prefix
