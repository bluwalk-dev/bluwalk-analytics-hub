SELECT
    a.booking_name,
    a.booking_state,
    a.booking_type,
    a.active as booking_is_active,
    a.booking_contract_type,
    b.contact_id as driver_contact_id,
    b.user_id as driver_user_id,
    a.vehicle_license_plate,
    a.vehicle_name,
    a.booking_pickup_datetime,
    a.booking_pickup_station_name,
    a.booking_rate_name,
    a.booking_rate_weekly_price,
    a.booking_mileage_limit,
    a.create_date AS booking_create_date
FROM {{ ref('fct_fleet_rental_bookings') }} a
LEFT JOIN {{ ref('dim_users') }} b ON a.driver_vat = b.user_vat