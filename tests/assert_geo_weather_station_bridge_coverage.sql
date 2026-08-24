{{ config(
    severity='warn'
) }}

-- Singular Test: assert_geo_weather_station_bridge_coverage
-- Calculates the percentage of zip code centroids where is_within_tolerance is false.
-- If more than 10% (0.10) of rows are out of tolerance, this test warns (does not
-- fail the build) — this is a data quality finding worth reporting, not a bug to hide.

with totals as (
    select
        count(*) as total_rows,
        count(case when is_within_tolerance = false then 1 end) as out_of_tolerance_rows
    from {{ ref('geo_weather_station_bridge') }}
)

select
    total_rows,
    out_of_tolerance_rows,
    (out_of_tolerance_rows::float / total_rows::float) as pct_out_of_tolerance
from totals
where (out_of_tolerance_rows::float / total_rows::float) > 0.10
