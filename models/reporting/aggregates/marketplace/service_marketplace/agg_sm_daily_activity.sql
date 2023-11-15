SELECT 
    *
FROM (
    SELECT * FROM {{ ref('int_user_fuel_energy_activity') }}
    UNION ALL
    SELECT * FROM {{ ref('int_user_vehicle_connected_activity') }}
    UNION ALL
    SELECT * FROM {{ ref('int_user_vehicle_rental_activity') }}
)
ORDER BY date DESC