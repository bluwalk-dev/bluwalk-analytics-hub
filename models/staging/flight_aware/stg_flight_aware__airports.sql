with

source as (
    SELECT *
    FROM {{ source('flight_aware', 'airports') }}
),

transformation as (

    select
        
        iata_code as airport_iata_code,
        name as airport_name,
        location_id as airport_location_id,
        city as airport_city,
        country as airport_country,
        CONCAT('https://waze.com/ul?ll=', CAST(Latitude AS FLOAT64), ',' , CAST(Longitude AS FLOAT64), '&navigate=yes') airport_navigation_link,
        latitude,
        longitude
        
    from source

)

select * from transformation
