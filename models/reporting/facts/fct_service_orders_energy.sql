SELECT
    transaction_key,
    partner_key,
    partner_name,
    energy_id,
    user_id,
    contact_id,
    card_name,
    start_date,
    end_date,
    energy_source,
    station_name,
    station_type,
    product,
    quantity,
    measurement_unit,
    cost,
    margin,
    statement
    
FROM (
    SELECT * FROM {{ ref('base_service_orders_fuel') }}
    UNION ALL
    SELECT * FROM {{ ref('base_service_orders_electricity') }}
    )
ORDER BY end_date DESC