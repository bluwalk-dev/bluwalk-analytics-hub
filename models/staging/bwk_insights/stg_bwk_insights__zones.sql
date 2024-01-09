with

source as (
    SELECT *
    FROM {{ source('generic', 'zones') }}
),

transformation as (

    select
        
        CAST(ID AS INT64) zone_id,
        CAST(Zone as STRING) zone_code,
        CAST(Location_ID as INT64) location_id,
        CAST(Name as STRING) zone_name,
        CAST(Latitude AS FLOAT64) latitude,
        CAST(Longitude AS FLOAT64) longitude,
        CAST(Status as STRING) status

    from source

)

select * from transformation
