WITH 
hubspot_booking_created as (
    SELECT
        user_id,
        vehicle_rental_license_plate,
        vehicle_rental_rate,
        vehicle_rental_pickup_date,
        vehicle_rental_pickup_station,
        vehicle_rental_booking_type,
    FROM {{ ref("fct_deals") }}
    WHERE deal_pipeline_stage_id = '307257316'
),
latest_vehicle_bookings AS (
    SELECT * FROM
        (SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY driver_user_id 
                ORDER BY booking_create_date DESC
            ) AS __row_number
        FROM {{ ref("dim_vehicle_bookings") }}
        --WHERE booking_is_active IS TRUE
        )
    WHERE __row_number = 1
)

SELECT
    a.user_id,
    c.user_email,
    b.vehicle_license_plate,
    b.booking_rate_name,
    CAST(b.booking_pickup_datetime AS STRING) booking_pickup_datetime,
    b.booking_pickup_station_name,
    b.booking_type
FROM hubspot_booking_created a
LEFT JOIN latest_vehicle_bookings b ON a.user_id = b.driver_user_id
LEFT JOIN {{ ref("dim_users") }} c ON a.user_id = c.user_id
WHERE
    (vehicle_rental_license_plate != vehicle_license_plate OR vehicle_rental_license_plate IS NULL) OR
    (vehicle_rental_rate != booking_rate_name OR vehicle_rental_rate IS NULL) OR
    (vehicle_rental_pickup_date != CAST(b.booking_pickup_datetime AS STRING) OR vehicle_rental_pickup_date IS NULL) OR
    (vehicle_rental_pickup_station != booking_pickup_station_name OR vehicle_rental_pickup_station IS NULL) OR
    (vehicle_rental_booking_type != booking_type OR vehicle_rental_booking_type IS NULL)