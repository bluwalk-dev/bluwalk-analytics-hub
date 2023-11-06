SELECT 
	fv.id vehicle_id,
	c.country_name as vehicle_country,
	fv.license_plate vehicle_plate,
	fv.vin_sn vehicle_vin,
	fvmb.name vehicle_brand,
	fvm.name vehicle_model,
	fv.vehicle_model_version,	
	fv.transmission vehicle_transmission,
    CASE 
        WHEN fv.transmission = 'manual' THEN 'M'
        WHEN fv.transmission = 'automatic' THEN 'A'
        ELSE NULL 
    END vehicle_transmission_code,
    fv.fuel_type vehicle_fuel_type,
    CASE 
        WHEN fv.fuel_type = 'diesel' THEN 'D'
        WHEN fv.fuel_type = 'electric' THEN 'E'
        WHEN fv.fuel_type = 'hybrid' THEN 'H'
        WHEN fv.fuel_type = 'lpg' THEN 'L'
        WHEN fv.fuel_type = 'gasoline' THEN 'G'
        ELSE NULL 
    END vehicle_fuel_type_code,
    fv.color vehicle_color,
	fv.seats vehicle_nr_seats,
	fv.doors vehicle_nr_doors
FROM {{ ref('stg_odoo__fleet_vehicles') }} fv
left join {{ ref('stg_odoo__fleet_vehicle_models') }} fvm on fv.model_id = fvm.id
left join {{ ref('stg_odoo__fleet_vehicle_model_brands') }} fvmb on fv.brand_id = fvmb.id
left join {{ ref('dim_countries') }} c on fv.vehicle_country_id = c.country_id
where active is TRUE