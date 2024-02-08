with

source as (
    SELECT *
    FROM {{ source('flight_aware', 'airports') }}
),

transformation as (

    select
        
        iata_code as airport_iata_code,
        name as airport_name,
        city as airport_city,
        country as airport_country,
        location_id as airport_location_id
        

    from source

)

select * from transformation
