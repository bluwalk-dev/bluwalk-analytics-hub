with

source as (
    select
        *
    from {{ source('uber', 'driver_quality') }}
),

transformation as (

    SELECT DISTINCT 
        CAST(driverUUID AS STRING) as partner_account_uuid,
        CAST(firstName AS STRING) as first_name,
        CAST(lastName AS STRING) as last_name,
        CAST(nrTrips AS INT) as nr_trips,
        CAST(acceptanceRate AS NUMERIC) as acceptance_rate,
        CAST(cancellationRate AS NUMERIC) as cancellation_rate,
        CAST(conclusionRate AS NUMERIC) as conclusion_rate,
        CAST(rating4weeks AS NUMERIC) as rating_4_weeks,
        CAST(rating500trips AS NUMERIC) as rating_last_500_trips,
        CAST(date AS DATE) as date
    FROM source

)

select * from transformation