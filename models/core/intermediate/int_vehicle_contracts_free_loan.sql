WITH vehicles AS (
    SELECT 
        fv.vehicle_id,  -- Unique identifier for the vehicle
        c.country_name as vehicle_country,  -- Country name where the vehicle is registered
        fv.vehicle_plate,  -- License plate number of the vehicle
        fv.vehicle_vin,  -- Vehicle Identification Number (VIN)
        fvmb.name as vehicle_brand,  -- Brand name of the vehicle
        fvm.name as vehicle_model,  -- Model name of the vehicle

        fv.current_driver_contract_id,

        -- Concatenating brand and model for a full name representation
        CONCAT(fvmb.name, ' ', fvm.name) as vehicle_brand_model,

        fv.vehicle_model_version,  -- Version of the vehicle model
        fv.vehicle_transmission,  -- Transmission type of the vehicle

        fv.vehicle_transmission_code,

        fv.vehicle_fuel_type,  -- Fuel type of the vehicle
        fv.vehicle_fuel_type_code,

        fv.vehicle_color,  -- Color of the vehicle
        fv.vehicle_nr_seats,  -- Number of seats in the vehicle
        fv.vehicle_nr_doors  -- Number of doors in the vehicle

    FROM bluwalk-analytics-hub.staging.stg_odoo_bw_fleet_vehicles  fv  -- Source table: staged fleet vehicles data
    LEFT JOIN bluwalk-analytics-hub.staging.stg_odoo_bw_fleet_vehicle_models fvm ON fv.model_id = fvm.id  -- Joining with vehicle models
    LEFT JOIN bluwalk-analytics-hub.staging.stg_odoo_bw_fleet_vehicle_model_brands fvmb ON fv.brand_id = fvmb.id  -- Joining with vehicle brands
    LEFT JOIN {{ ref('dim_countries') }} c ON fv.vehicle_country_id = c.country_id  -- Joining with country dimension table
)

SELECT 
    TO_HEX(MD5(rc.name || 'Free Loan')) as vehicle_contract_key,
    --rc.id as vehicle_contract_id,  -- Unique identifier for the rental contract
    rc.name as vehicle_contract_name,
    fv.vehicle_id,  -- Identifier for the associated vehicle
    fv.vehicle_plate,  -- License plate number of the vehicle
    fv.vehicle_brand_model,  -- Concatenated brand and model of the vehicle
    fv.vehicle_brand,  -- Brand of the vehicle
    fv.vehicle_model,  -- Model of the vehicle

    -- Contact and user details
    aaa.analytic_account_owner_contact_id as contact_id,  -- Contact ID associated with the billing account
    u.user_id,  -- User ID associated with the contact

    -- Contract details
    rc.state as vehicle_contract_state,  -- Current state of the rental contract
    start_date,  -- Start date of the rental contract
    end_date,  -- End date of the rental contract
    start_kms,  -- Starting kilometers on the vehicle at the start of the contract
    end_kms,  -- Ending kilometers on the vehicle at the end of the contract

    -- Pricing and mileage details
    round(mileage_limit*7,0) as mileage_included_weekly,  -- Weekly mileage included in the contract
    round(mileage_excess,2) as mileage_excess_price,  -- Price per excess mileage
    round(rate_base_value*7,0) as rent_weekly,  -- Weekly base rent for the vehicle

    -- Supplier and vehicle segment details
    c.partner_name as supplier_name,  -- Short name of the supplier
    s.name as vehicle_segment_name,  -- Name of the vehicle segment
    vc.name as vehicle_code,  -- Code of the vehicle category
    vc.category as vehicle_category_letter,  -- Letter representing the vehicle category
    vc.vehicle_category_type as vehicle_body_letter,  -- Letter representing the vehicle body type
    fv.vehicle_transmission,  -- Transmission type of the vehicle

    -- Transmission and fuel type details
    IFNULL(vc.transmission, fv.vehicle_transmission) as vehicle_transmission_letter,  -- Letter representing the transmission type
    CASE 
        WHEN IFNULL(vc.fuel, fv.vehicle_fuel_type_code) = 'D' THEN 'diesel'
        WHEN IFNULL(vc.fuel, fv.vehicle_fuel_type_code) IN ('E','F') THEN 'electric'
        WHEN IFNULL(vc.fuel, fv.vehicle_fuel_type_code) = 'H' THEN 'hybrid'
        WHEN IFNULL(vc.fuel, fv.vehicle_fuel_type_code) = 'L' THEN 'lpg'
        WHEN IFNULL(vc.fuel, fv.vehicle_fuel_type_code) = 'G' THEN 'gasoline'
        ELSE NULL 
    END as vehicle_fuel_type,
    IFNULL(vc.fuel, fv.vehicle_fuel_type_code) as vehicle_fuel_letter,  -- Letter representing the fuel type

    -- Contract type
    rc.rental_contract as vehicle_contract_type,  -- Type of the rental contract
    rc.service_fee,
    CASE
        WHEN rc.rental_contract = 'free_loan' THEN 7
        ELSE 6
    END service_partner_id

FROM {{ ref('stg_odoo__rental_contracts') }} rc  -- Source table: staged rental contracts data
LEFT JOIN {{ ref('stg_odoo__rate_bases') }} rb ON rc.rate_base_id = rb.id  -- Joining with rate bases
LEFT JOIN {{ ref('dim_partners') }} c ON rb.partner_id = c.partner_contact_id  -- Joining with contacts for supplier details
LEFT JOIN {{ ref('stg_odoo__vehicle_categories') }} vc ON rb.vehicle_category_id = vc.id  -- Joining with vehicle categories
LEFT JOIN bluwalk-analytics-hub.staging.stg_odoo_bw_segments s ON vc.segment_id = s.id  -- Joining with segments for vehicle segment details
LEFT JOIN {{ ref('dim_accounting_analytic_accounts') }} aaa ON rc.billing_account_id = aaa.analytic_account_id  -- Joining with accounting analytic accounts
LEFT JOIN vehicles fv ON rc.vehicle_id = fv.vehicle_id  -- Joining with vehicles for detailed vehicle information
LEFT JOIN {{ ref('dim_users') }} u ON aaa.analytic_account_owner_contact_id = u.contact_id  -- Joining with users for user details
WHERE
    rc.active IS TRUE AND
    (c.partner_category = 'Vehicles' OR c.partner_category IS NULL) AND
    rc.rental_contract = 'free_loan'
ORDER BY start_date DESC
