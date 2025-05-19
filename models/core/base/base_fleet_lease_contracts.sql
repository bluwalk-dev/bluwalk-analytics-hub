WITH contract_conditions AS (
    select 
        lease_contract_id, 
        min(effective_date) as effective_date,
        case 
            when count(termination_date) < count(*) then null 
            else max(termination_date) 
        end as termination_date
    from {{ ref('stg_odoo_drivfit__lease_contract_conditions') }}
    group by lease_contract_id
)

SELECT
    a.id as lease_contract_id,
    a.name as lease_contract_name,
    a.state as lease_contact_state,
    a.customer_id,
    driver_id,
    a.booking_id,
    b.vehicle_id,
    b.vehicle_license_plate,
    b.vehicle_deal_id,
    b.vehicle_deal_name, -- Atenção que talvez tenhamos o mesmo carro em mais do que um deal
    b.vehicle_name,
    c.station_name as start_station,
    d.station_name as end_station,
    a.start_inspection_id,
    a.end_inspection_id,
    CAST(start_date AS DATE) start_date,
    CAST(end_date AS DATE) end_date,
    CAST(cc.effective_date AS DATE) billing_start_date,
    CAST(cc.termination_date AS DATE) billing_end_date,
    a.end_date_estimated,
    a.start_kms,
    a.end_kms,
    
    j.rate_base_id,
    j.rate_base_wk_price as wk_rent_price,
    j.rate_mileage_wk_limit as wk_included_mileage,
    k.mileage_excess as extra_mileage_price,

    -- Transmission and fuel type details
    IFNULL(i.transmission, b.vehicle_transmission) as vehicle_transmission_letter,  -- Letter representing the transmission type
    CASE 
        WHEN IFNULL(i.fuel, b.vehicle_fuel_type_code) = 'D' THEN 'diesel'
        WHEN IFNULL(i.fuel, b.vehicle_fuel_type_code) IN ('E','F') THEN 'electric'
        WHEN IFNULL(i.fuel, b.vehicle_fuel_type_code) = 'H' THEN 'hybrid'
        WHEN IFNULL(i.fuel, b.vehicle_fuel_type_code) = 'L' THEN 'lpg'
        WHEN IFNULL(i.fuel, b.vehicle_fuel_type_code) = 'G' THEN 'gasoline'
        ELSE NULL 
    END as vehicle_fuel_type,
    IFNULL(i.fuel, b.vehicle_fuel_type_code) as vehicle_fuel_letter

FROM {{ ref('stg_odoo_drivfit__lease_contracts') }} a
LEFT JOIN {{ ref('dim_fleet_vehicles') }} b ON a.vehicle_id = b.vehicle_id
LEFT JOIN {{ ref('dim_fleet_stations') }} c ON a.start_station_id = c.station_id
LEFT JOIN {{ ref('dim_fleet_stations') }} d ON a.end_station_id = d.station_id
LEFT JOIN {{ ref('stg_odoo_drivfit__lease_contract_conditions') }} j ON a.conditions_id = j.id
LEFT JOIN {{ ref('stg_odoo_drivfit__rate_bases') }} h ON j.rate_base_id = h.id
LEFT JOIN {{ ref('stg_odoo_drivfit__rate_mileages') }} k ON j.rate_mileage_id = k.id
LEFT JOIN {{ ref('stg_odoo_drivfit__vehicle_categories') }} i ON h.vehicle_category_id = i.id  -- Joining with vehicle categories
LEFT JOIN contract_conditions cc ON cc.lease_contract_id = a.id
WHERE a.active is true