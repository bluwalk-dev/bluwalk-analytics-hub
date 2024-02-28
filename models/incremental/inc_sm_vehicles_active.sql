WITH hubspot_list AS (
    SELECT 
        a.user_id,
        b.user_email,
        a.active_vehicle_contracts 
    FROM {{ ref('base_hubspot_contacts') }} a
    LEFT JOIN {{ ref('dim_users') }} b ON a.user_id = b.user_id
    WHERE active_vehicle_contracts = TRUE
), new_list AS (
    SELECT
        a.user_id,
        b.user_email,
        a.vehicle_plate,
        a.vehicle_brand_model,
        a.vehicle_fuel_type,
        a.vehicle_contract_type,
        TRUE active_vehicle_contracts
    FROM {{ ref("dim_vehicle_contracts") }} a
    LEFT JOIN  {{ ref("dim_users") }} b ON a.user_id = b.user_id
    WHERE vehicle_contract_state = 'open'
)

SELECT * FROM new_list

UNION ALL

SELECT

    a.user_id,
    a.user_email,
    NULL vehicle_plate,
    NULL vehicle_brand_model,
    NULL vehicle_fuel_type,
    NULL vehicle_contract_type,
    FALSE active_vehicle_contracts

FROM hubspot_list a
LEFT JOIN new_list b ON a.user_id = b.user_id
WHERE b.user_id IS NULL

