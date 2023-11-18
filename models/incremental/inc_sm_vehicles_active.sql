SELECT
    a.user_id,
    b.user_email,
    a.vehicle_plate,
    a.vehicle_brand_model,
    a.vehicle_fuel_type,
    a.vehicle_contract_type
FROM {{ ref("dim_vehicle_contracts") }} a
LEFT JOIN  {{ ref("dim_users") }} b ON a.user_id = b.user_id
WHERE vehicle_contract_state = 'open'