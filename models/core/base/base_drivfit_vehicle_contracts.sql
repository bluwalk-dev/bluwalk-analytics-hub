WITH analytic_accounts AS (
    SELECT *
    FROM {{ ref('dim_accounting_analytic_accounts') }}
    WHERE analytic_account_type = 'User'
)
SELECT
    TO_HEX(MD5(vehicle_contract_name || 'Drivfit')) as vehicle_contract_key,
    --a.id as vehicle_contract_id,  -- Unique identifier for the rental contract
    a.vehicle_contract_name,
    b.vehicle_id,  -- Identifier for the associated vehicle
    b.vehicle_plate,  -- License plate number of the vehicle
    b.vehicle_brand_model,  -- Concatenated brand and model of the vehicle
    b.vehicle_brand,  -- Brand of the vehicle
    b.vehicle_model,  -- Model of the vehicle

    c.contact_id,  -- Contact ID associated with the billing account
    c.user_id,  -- User ID associated with the contact

    vehicle_contract_state,  -- Current state of the rental contract
    start_date,  -- Start date of the rental contract
    end_date,  -- End date of the rental contract
    start_kms,  -- Starting kilometers on the vehicle at the start of the contract
    end_kms,  -- Ending kilometers on the vehicle at the end of the contract

    mileage_included_weekly,
    mileage_excess_price,
    rent_weekly,

    supplier_name,  -- Short name of the supplier
    '' as vehicle_segment_name,  -- Name of the vehicle segment
    '' as vehicle_code,  -- Code of the vehicle category
    '' as vehicle_category_letter,  -- Letter representing the vehicle category
    '' as vehicle_body_letter,  -- Letter representing the vehicle body type
    b.vehicle_transmission,  -- Transmission type of the vehicle

    -- Transmission and fuel type details
    b.vehicle_transmission as vehicle_transmission_letter,  -- Letter representing the transmission type
    CASE 
        WHEN b.vehicle_fuel_type_code = 'D' THEN 'diesel'
        WHEN b.vehicle_fuel_type_code IN ('E','F') THEN 'electric'
        WHEN b.vehicle_fuel_type_code = 'H' THEN 'hybrid'
        WHEN b.vehicle_fuel_type_code = 'L' THEN 'lpg'
        WHEN b.vehicle_fuel_type_code = 'G' THEN 'gasoline'
        ELSE NULL 
    END as vehicle_fuel_type,
    b.vehicle_fuel_type_code as vehicle_fuel_letter,  -- Letter representing the fuel type

    -- Contract type
    vehicle_contract_type,  -- Type of the rental contract
    5 as service_fee,
    6 as service_partner_id
    
FROM bluwalk-analytics-hub.core.core_fleet_rental_contracts a
LEFT JOIN bluwalk-analytics-hub.core.core_vehicles b ON a.vehicle_plate = b.vehicle_plate
LEFT JOIN bluwalk-analytics-hub.core.core_users c ON a.contact_vat = c.user_vat
LEFT JOIN analytic_accounts d ON c.contact_id = d.analytic_account_owner_contact_id
WHERE a.customer_id = 21
ORDER BY start_date DESC