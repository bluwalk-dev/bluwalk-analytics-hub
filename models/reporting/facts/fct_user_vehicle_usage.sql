SELECT
    user_id,
    vehicle_contract_type,
    vehicle_plate,
    vehicle_contract_key,
    MIN(dropoff_timestamp) start_timestamp,
    MAX(dropoff_timestamp) end_timestamp
FROM {{ ref('fct_user_rideshare_trips') }}
WHERE 
    user_id is not null AND 
    vehicle_contract_key is not null
GROUP BY user_id, vehicle_contract_type, vehicle_contract_key, vehicle_plate
ORDER BY end_timestamp DESC