SELECT DISTINCT
    CAST(driverName AS STRING) as driver_name,
    CAST(driverPhone AS STRING) as driver_phone,
    CAST(paymentDate AS DATE) payment_date,
    DATETIME(paymentDate, paymentTime) as payment_local_time,
    TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S',PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(DATETIME(paymentDate, paymentTime) AS STRING), 'Europe/Lisbon'))) as payment_timestamp,
    CAST(paymentMethod AS STRING) payment_method,
    CAST(pickupAddress AS STRING) address_pickup,
    CAST(requestDate AS DATE) request_date,
    TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S',PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(DATETIME(requestDate, requestTime) AS STRING), 'Europe/Lisbon'))) as request_timestamp,
    DATETIME(requestDate, requestTime) as request_local_time,
    CAST(tripValue AS NUMERIC) trip_value,
    CAST(tripBookingFee AS NUMERIC) trip_booking_fee,
    CAST(tripTolls AS NUMERIC) trip_tolls,
    CAST(tripCancellationFee AS NUMERIC) trip_cancellation_fee,
    CAST(tripTips AS NUMERIC) trip_tip,
    CAST(tripStatus AS STRING) trip_status,
    CAST(vehicleModel AS STRING) vehicle_model,
    CAST(replace(vehiclePlate,'-','') AS STRING) vehicle_plate
FROM {{ source('bolt', 'daily_report_trips') }}
ORDER BY request_local_time DESC