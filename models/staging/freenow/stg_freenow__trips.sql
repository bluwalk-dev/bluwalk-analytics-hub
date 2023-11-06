with

source as (
    SELECT DISTINCT
        CAST(booking_id AS INT64) trip_id,
        CAST(company_name as STRING) company_name,
        CAST(driver_id AS STRING) partner_account_uuid,
        CAST(driver_first_name AS STRING) first_name,
        CAST(driver_last_name AS STRING) last_name,
        CONCAT(driver_first_name, ' ', driver_last_name) driver_name,
        CAST(pickup_location AS STRING) address_pickup,
        CAST(dropoff_location AS STRING) address_dropoff,
        CAST(pickup_date AS DATETIME) request_local_time,
        TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S',PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(CAST(pickup_date AS DATETIME) AS STRING), 'Europe/Lisbon'))) as request_timestamp,
        CAST(closed_date AS DATETIME) dropoff_local_time,
        TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S',PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(CAST(closed_date AS DATETIME) AS STRING), 'Europe/Lisbon'))) as dropoff_timestamp,
        CAST(tour_currency AS STRING) trip_currency,
        CAST(tour_value AS NUMERIC) trip_value,
        CAST(tour_tip AS NUMERIC) trip_tip,
        CAST(toll_value AS NUMERIC) trip_toll,
        CAST(tax_percentage AS STRING) tax_percentage,
        CAST(payment_method AS STRING) payment_method,
        CAST(booking_state AS STRING) trip_status,
        CAST(hailing_type AS STRING) hailing_type,
        CAST(service AS STRING) service_type,
        CAST(fare_type AS STRING) fare_type,
        CAST(replace(licence_plate,'-','') AS STRING) vehicle_plate,
        CAST(loadTimestampUTC AS INT) load_timestamp
    FROM {{ source('free_now', 'booking_history') }}
),

transformation as (

    select
        
        *

    from source

)

select * from transformation
