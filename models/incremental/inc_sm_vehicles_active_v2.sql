SELECT 
    c.user_email,
    a.vehicle_contract_type,
    a.vehicle_plate,
    a.vehicle_brand_model,
    a.vehicle_fuel_type,
    b.active_vehicle_contracts
FROM 
    (SELECT *
     FROM {{ ref('dim_vehicle_contracts') }} 
     WHERE vehicle_contract_state = 'open') a
FULL OUTER JOIN
    (SELECT user_id, active_vehicle_contracts 
     FROM {{ ref('base_hubspot_contacts') }} 
     WHERE user_id IS NOT NULL) b 
ON a.user_id = b.user_id
LEFT JOIN {{ ref('dim_users') }} c on COALESCE(a.user_id, b.user_id) = c.user_id
WHERE 
    (a.vehicle_contract_type IS NOT NULL AND b.active_vehicle_contracts = FALSE) 
    OR 
    (b.active_vehicle_contracts = TRUE AND a.vehicle_contract_type IS NULL)
ORDER BY user_id