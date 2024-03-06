with

source as (
    SELECT *
    FROM {{ source('generic', 'zip_codes') }}
),

transformation as (

    select
        
        zip_code,
        location_id,
        district,
        county,
        parish,
        CAST(latitude AS NUMERIC) latitude,
        CAST(longitude AS NUMERIC) longitude,
        zone_id

    from source

)

select * from transformation
