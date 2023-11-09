with

source as (
    select
        *
    from {{ source('uber', 'driver_status') }}
),

transformation as (

    SELECT 

        CAST(driverUuid AS STRING) driver_uuid,
        CAST(firstName AS STRING) first_name,
        CAST(lastName AS STRING) last_name,
        CAST(phone AS STRING) phone,
        CAST(email AS STRING) email,
        CAST(licensePlate AS STRING) vehicle_plate,
        CAST(onboardingStatus AS STRING) onboarding_status,
        status,
        timestamp,
        CAST(loadTimestamp AS INT64) AS load_timestamp

    FROM source

)

select * from transformation