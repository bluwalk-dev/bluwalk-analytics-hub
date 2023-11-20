SELECT
    energy_id,
    user_id,
    contact_id,
    card_name,
    start_date,
    end_date,
    energy_source,
    station_name,
    station_type,
    supplier_contact_id,
    supplier_name,
    product,
    quantity,
    measurement_unit,
    cost,
    margin,
    price,
    statement
FROM (
    SELECT * FROM {{ ref('base_service_orders_fuel') }}
    UNION ALL
    SELECT * FROM {{ ref('base_service_orders_electricity') }}
    )
ORDER BY end_date DESC