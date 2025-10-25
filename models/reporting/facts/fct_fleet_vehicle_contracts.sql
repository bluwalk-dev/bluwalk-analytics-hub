SELECT
    a.id,
    a.name,
    a.active,
    a.start_date,
    a.expiration_date as end_date,
    a.state,
    c.vehicle_id,
    c.vehicle_license_plate,
    c.vehicle_vin,
    c.vehicle_brand,
    c.vehicle_model
FROM {{ ref('stg_odoo_drivfit__fleet_vehicle_log_contracts') }} a
LEFT JOIN {{ ref('stg_odoo_drivfit__fleet_vehicle_costs') }} b ON a.cost_id = b.id
LEFT JOIN {{ ref('dim_fleet_vehicles') }} c ON b.vehicle_id = c.vehicle_id

UNION ALL

SELECT
    a.id,
    a.name,
    a.active,
    a.start_date,
    a.expiration_date as end_date,
    a.state,
    c.vehicle_id,
    c.vehicle_plate,
    c.vehicle_vin,
    c.vehicle_brand,
    c.vehicle_model
FROM {{ ref('stg_odoo__fleet_vehicle_log_contracts') }} a
LEFT JOIN {{ ref('stg_odoo__fleet_vehicle_costs') }} b ON a.cost_id = b.id
LEFT JOIN bluwalk-analytics-hub.core.core_vehicles c ON b.vehicle_id = c.vehicle_id