with

source as (
    select
        *
    from {{ source('uber', 'trip_activity') }}
),

transformation as (

    SELECT DISTINCT 
        CAST(tripUUID AS STRING) AS trip_uuid,
        CAST(driverUUID AS STRING) AS partner_account_uuid,
        CAST(firstName AS STRING) AS first_name,
        CAST(lastName AS STRING) AS last_name,
        CONCAT(firstName, ' ', lastName) driver_name,
        CAST(vehicleUUID AS STRING) AS vehicle_uuid,
        CAST(replace(left(vehiclePlate,8),'-','') AS STRING) AS vehicle_plate,
        CAST(serviceType AS STRING) AS service_type,

        TIMESTAMP(DATETIME(requestTimestamp), 'Europe/Lisbon') AS request_timestamp,
        DATETIME(TIMESTAMP(DATETIME(requestTimestamp), 'Europe/Lisbon'), 'Europe/Lisbon') as request_local_time,

        TIMESTAMP(DATETIME(dropOffTimestamp), 'Europe/Lisbon') AS dropoff_timestamp,
        DATETIME(TIMESTAMP(DATETIME(dropOffTimestamp), 'Europe/Lisbon'), 'Europe/Lisbon') as dropoff_local_time,

        CAST(pickupAddress AS STRING) AS address_pickup,
        CAST(dropOffAddress AS STRING) AS address_dropoff,
        CAST(tripDistance AS NUMERIC) AS trip_distance,
        CAST(tripStatus AS STRING) AS trip_status,
        CAST(orgId AS STRING) as partner_login_id
    FROM source

)

select * from bluwalk-analytics-hub.staging.stg_uber_trips