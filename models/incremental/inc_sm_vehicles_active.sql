SELECT
    user_id,
    vehicle_plate,
    vehicle_brand_model,
    vehicle_fuel_type,
    vehicle_contract_type
FROM {{ ref("dim_vehicle_contracts") }}
WHERE vehicle_contract_state = 'open'