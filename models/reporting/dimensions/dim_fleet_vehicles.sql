select
    id as vehicle_id,
    license_plate as vehicle_license_plate,
    vin_sn as vehicle_vin,

    c.vehicle_brand_id,
    c.vehicle_brand,
    c.vehicle_model_id,
    c.vehicle_model,
    c.vehicle_name,

    b.vehicle_deal_id,
    b.vehicle_deal_name,
    
    a.billing_account_id analytic_account_id,

    -- Transmission code: 'M' for manual, 'A' for automatic, NULL for others
    a.transmission as vehicle_transmission,
    CASE 
        WHEN a.transmission = 'manual' THEN 'M'
        WHEN a.transmission = 'automatic' THEN 'A'
        ELSE NULL 
    END as vehicle_transmission_code,

    a.fuel_type as vehicle_fuel_type,  -- Fuel type of the vehicle

    -- Fuel type code: 'D' for diesel, 'E' for electric, etc.
    CASE 
        WHEN a.fuel_type = 'diesel' THEN 'D'
        WHEN a.fuel_type = 'electric' THEN 'E'
        WHEN a.fuel_type = 'hybrid' THEN 'H'
        WHEN a.fuel_type = 'lpg' THEN 'L'
        WHEN a.fuel_type = 'gasoline' THEN 'G'
        ELSE NULL 
    END as vehicle_fuel_type_code,

    a.color as vehicle_color,  -- Color of the vehicle
    a.seats as vehicle_nr_seats,  -- Number of seats in the vehicle
    a.doors as vehicle_nr_doors  -- Number of doors in the vehicle

FROM {{ ref('stg_odoo_drivfit__fleet_vehicles') }} a
LEFT JOIN {{ ref('dim_fleet_vehicle_deals') }} b ON a.deal_id = b.vehicle_deal_id
LEFT JOIN {{ ref('dim_fleet_vehicle_models') }} c ON a.model_id = c.vehicle_model_id