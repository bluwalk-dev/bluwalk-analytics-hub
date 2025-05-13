WITH rental_contracts AS (
    SELECT
        a.id as rental_contract_id,
        a.name as rental_contract_name,
        a.state as rental_contact_state,
        a.parent_id,
        a.driver_id,
        e.contact_name driver_name,
        f.analytic_account_owner_contact_id customer_id,
        g.contact_name customer_name,
        b.vehicle_id,
        b.vehicle_license_plate,
        b.vehicle_deal_id,
        b.vehicle_deal_name,
        b.vehicle_name,
        c.station_name as start_station,
        d.station_name as end_station,
        a.start_inspection_id,
        a.end_inspection_id,
        a.start_date,
        a.end_date,
        a.start_kms,
        a.end_kms,
        a.active,
        a.rate_base_value as daily_base_price,
        a.protection_daily_value as daily_protection_price,
        a.mileage_value as daily_mileage_price,
        a.mileage_limit as daily_mileage_limit,
        a.mileage_excess as mileage_excess_price,

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
        
    FROM {{ ref('stg_odoo_drivfit__rental_contracts') }} a
    LEFT JOIN {{ ref('dim_fleet_vehicles') }} b ON a.vehicle_id = b.vehicle_id

    LEFT JOIN {{ ref('dim_fleet_stations') }} c ON a.start_station_id = c.station_id
    LEFT JOIN {{ ref('dim_fleet_stations') }} d ON a.end_station_id = d.station_id

    LEFT JOIN {{ ref('int_odoo_drivfit_contacts') }} e ON a.driver_id = e.contact_id
    LEFT JOIN {{ ref('int_odoo_drivfit_analytic_accounts') }} f ON a.billing_account_id = f.analytic_account_id
    LEFT JOIN {{ ref('int_odoo_drivfit_contacts') }} g ON f.analytic_account_owner_contact_id = g.contact_id

    LEFT JOIN {{ ref('stg_odoo_drivfit__rate_bases') }} h ON a.rate_base_id = h.id
    LEFT JOIN {{ ref('stg_odoo_drivfit__vehicle_categories') }} i ON h.vehicle_category_id = i.id  -- Joining with vehicle categories

    WHERE a.active is true
),

lease_contracts AS (
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
)


SELECT * FROM (
    select
        rental_contract_id as vehicle_contract_id,
        rental_contract_name as vehicle_contract_name,
        vehicle_license_plate as vehicle_plate,
        customer_id,
        b.contact_vat,
        rental_contact_state as vehicle_contract_state,
        start_date,
        end_date,
        start_kms,
        end_kms,
        ROUND((daily_mileage_limit * 7),0) as mileage_included_weekly,
        mileage_excess_price,
        ROUND((daily_base_price * 7),0) as rent_weekly,
        'Drivfit' as supplier_name,
        'car_rental' vehicle_contract_type,
        'short-term' vehicle_contract_length,
        vehicle_transmission_letter,
        vehicle_fuel_type,
        vehicle_fuel_letter
    from rental_contracts a
    left join {{ ref('int_odoo_drivfit_contacts') }} b on a.driver_id = b.contact_id

    UNION ALL

    select
        lease_contract_id as vehicle_contract_id,
        lease_contract_name as vehicle_contract_name,
        vehicle_license_plate as vehicle_plate,
        customer_id,
        b.contact_vat,
        lease_contact_state as vehicle_contract_state,
        start_date,
        end_date,
        start_kms,
        end_kms,
        wk_included_mileage as mileage_included_weekly,
        extra_mileage_price as mileage_excess_price,
        wk_rent_price as rent_weekly,
        'Drivfit' as supplier_name,
        'car_rental' vehicle_contract_type,
        'mid-term' vehicle_contract_length,
        vehicle_transmission_letter,
        vehicle_fuel_type,
        vehicle_fuel_letter
    from lease_contracts a
    left join {{ ref('int_odoo_drivfit_contacts') }} b on a.driver_id = b.contact_id
)
ORDER BY start_date DESC