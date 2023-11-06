with

source as (
    select
        *
    from {{ source('uber', 'driver_activity') }}
),

transformation as (

    SELECT DISTINCT 
        CAST(driverUUID AS STRING) as partner_account_uuid,
        CAST(firstName AS STRING) as first_name,
        CAST(lastName AS STRING) as last_name, 
        CAST(IFNULL(nrTrips,0) AS INT) as nr_trips, 
        CAST(IFNULL(onlineMinutes,0) AS INT) as online_minutes,
        CAST(IFNULL(tripMinutes, 0) AS INT) as trip_minutes,
        CAST(date AS DATE) as date
    FROM source

)

select * from transformation