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
        CAST(closest_zip_code AS STRING) AS zip_code,
        CAST(country AS STRING) AS country,
        CASE
            WHEN CAST(distance_to_zip_code AS NUMERIC) > 2000 THEN 'good'
            ELSE 'bad'
        END as zip_accuracy,
        CAST(load_timestamp AS STRING) AS load_timestamp

    from source

)

select * from transformation
where geo_accuracy = 'good' and zip_accuracy = 'good'
