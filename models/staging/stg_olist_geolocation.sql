{{ config(materialized='view') }}

-- Intentionally NOT deduplicated here — many lat/lng pings per zip_code_prefix.
-- Centroid resolution (median lat/lng per prefix) happens in the transform
-- layer, not staging. See stg_geo_zip_centroid in the transform models.

with source as (
    select * from {{ source('raw_logistics', 'raw_olist_geolocation') }}
),

renamed as (
    select
        geolocation_zip_code_prefix::string as geolocation_zip_code_prefix,
        geolocation_lat::float               as geolocation_lat,
        geolocation_lng::float               as geolocation_lng,
        geolocation_city,
        geolocation_state,
        current_timestamp() as _loaded_at
    from source
)

select * from renamed
