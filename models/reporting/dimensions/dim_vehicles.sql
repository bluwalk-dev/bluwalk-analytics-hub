-- Temporary to avoid duplication of vehicles
WITH vehicles AS (
    SELECT * FROM
        (SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY license_plate 
                ORDER BY create_date ASC
            ) AS __row_number
        FROM {{ ref("stg_odoo__fleet_vehicles") }} 
        )
    WHERE __row_number = 1
)

/*
  This model enriches the fleet vehicle data from the stg_odoo__fleet_vehicles source
  by joining it with vehicle models, brands, and country dimension tables. It provides
  a comprehensive view of vehicle details including model, brand, transmission type,
  fuel type, and associated country information.
*/

SELECT 
    fv.id as vehicle_id,  -- Unique identifier for the vehicle
    c.country_name as vehicle_country,  -- Country name where the vehicle is registered
    fv.license_plate as vehicle_plate,  -- License plate number of the vehicle
    fv.vin_sn as vehicle_vin,  -- Vehicle Identification Number (VIN)
    fvmb.name as vehicle_brand,  -- Brand name of the vehicle
    fvm.name as vehicle_model,  -- Model name of the vehicle
    d.name as vehicle_deal_name,

    fv.driver_id as current_driver_contract_id,

    -- Concatenating brand and model for a full name representation
    CONCAT(fvmb.name, ' ', fvm.name) as vehicle_brand_model,

    fv.vehicle_model_version,  -- Version of the vehicle model
    fv.transmission as vehicle_transmission,  -- Transmission type of the vehicle

    -- Transmission code: 'M' for manual, 'A' for automatic, NULL for others
    CASE 
        WHEN fv.transmission = 'manual' THEN 'M'
        WHEN fv.transmission = 'automatic' THEN 'A'
        ELSE NULL 
    END as vehicle_transmission_code,

    fv.fuel_type as vehicle_fuel_type,  -- Fuel type of the vehicle

    -- Fuel type code: 'D' for diesel, 'E' for electric, etc.
    CASE 
        WHEN fv.fuel_type = 'diesel' THEN 'D'
        WHEN fv.fuel_type = 'electric' THEN 'E'
        WHEN fv.fuel_type = 'hybrid' THEN 'H'
        WHEN fv.fuel_type = 'lpg' THEN 'L'
        WHEN fv.fuel_type = 'gasoline' THEN 'G'
        ELSE NULL 
    END as vehicle_fuel_type_code,

    fv.color as vehicle_color,  -- Color of the vehicle
    fv.seats as vehicle_nr_seats,  -- Number of seats in the vehicle
    fv.doors as vehicle_nr_doors  -- Number of doors in the vehicle

FROM vehicles fv  -- Source table: staged fleet vehicles data
LEFT JOIN {{ ref('stg_odoo__fleet_vehicle_models') }} fvm ON fv.model_id = fvm.id  -- Joining with vehicle models
LEFT JOIN {{ ref('stg_odoo__fleet_vehicle_model_brands') }} fvmb ON fv.brand_id = fvmb.id  -- Joining with vehicle brands
LEFT JOIN {{ ref('dim_countries') }} c ON fv.vehicle_country_id = c.country_id  -- Joining with country dimension table
LEFT JOIN {{ ref('stg_odoo__fleet_vehicle_deals') }} d ON fv.deal_id = d.id
WHERE active IS TRUE  -- Filtering only active vehicles