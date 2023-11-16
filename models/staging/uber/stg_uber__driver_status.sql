with

source as (
    select
        *
    from {{ source('uber', 'driver_status') }}
),

transformation as (

    SELECT 

        CAST(driverUuid AS STRING) partner_account_uuid,
        CAST(firstName AS STRING) first_name,
        CAST(lastName AS STRING) last_name,
        CAST(phone AS STRING) phone,
        CAST(email AS STRING) email,
        CAST(licensePlate AS STRING) vehicle_plate,
        CAST(onboardingStatus AS STRING) onboarding_status,
        status,
        timestamp,
        TIMESTAMP_MILLIS(loadTimestamp) AS extraction_ts

    FROM source

)

select * from transformation