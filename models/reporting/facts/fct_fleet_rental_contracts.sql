SELECT * FROM (
    SELECT
        rental_contract_id as vehicle_contract_id,
        rental_contract_name as vehicle_contract_name,
        vehicle_license_plate as vehicle_plate,
        vehicle_id,
        customer_id,
        b.contact_vat,
        rental_contact_state as vehicle_contract_state,
        start_date,
        end_date,
        NULL as end_date_estimated,
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
    from {{ ref('base_fleet_rental_contracts') }} a
    left join {{ ref('int_odoo_drivfit_contacts') }} b on a.driver_id = b.contact_id

    UNION ALL

    select
        lease_contract_id as vehicle_contract_id,
        lease_contract_name as vehicle_contract_name,
        vehicle_license_plate as vehicle_plate,
        vehicle_id,
        customer_id,
        b.contact_vat,
        lease_contact_state as vehicle_contract_state,
        start_date,
        end_date,
        end_date_estimated,
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
    from {{ ref('base_fleet_lease_contracts') }} a
    left join {{ ref('int_odoo_drivfit_contacts') }} b on a.driver_id = b.contact_id
)
ORDER BY start_date DESC