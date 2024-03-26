with

source as (
    select
        *
    from {{ source('uber', 'vehicles') }}
),

transformation as (

    SELECT
        CAST(id AS STRING) vehicle_id,
        CAST(owner_id AS STRING) vehicle_owner_id,
        LEFT(REPLACE(CAST(license_plate AS STRING),'-',''),6) vehicle_plate,
        LOWER(CAST(status AS STRING)) status,
        CAST(make AS STRING) make,
        CAST(model AS STRING) model,
        CAST(year AS INT64) year,
        CAST(vin AS STRING) vin,
        CAST(color_hex_code AS STRING) color_hex_code,
        CAST(color_name AS STRING) color_name,
        TIMESTAMP_MILLIS(last_updated) last_updated,
        TIMESTAMP_MILLIS(load_timestamp) load_timestamp
    FROM source
    WHERE load_timestamp IS NOT NULL
)

select * from transformation