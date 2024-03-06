with

source as (
    SELECT *
    FROM {{ source('generic', 'address_geocoding') }}
),

transformation as (

    select
        
        CAST(address AS STRING) AS address,
        CAST(latitude AS NUMERIC) AS latitude,
        CAST(longitude AS NUMERIC) AS longitude,
        CASE
            WHEN estimation_type IN ('ROOFTOP', 'GEOMETRIC_CENTER') THEN 'good'
            ELSE 'bad'
        END as geo_accuracy,
        CAST(country AS STRING) AS country,
        CAST(load_timestamp AS STRING) AS load_timestamp

    from source

)

select * from transformation
