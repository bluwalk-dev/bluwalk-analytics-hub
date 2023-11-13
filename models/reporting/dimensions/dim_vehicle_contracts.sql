SELECT 	
	rc.id vehicle_contract_id,
	fv.vehicle_id,
    fv.vehicle_plate,
    fv.vehicle_brand_model,
	fv.vehicle_brand,
	fv.vehicle_model,
	aaa.account_owner_contact_id contact_id,
    u.user_id,
	rc.state vehicle_contract_state,
	start_date start_date,
	end_date end_date,
	start_kms start_kms,
	end_kms end_kms,
	start_station_id,
	end_station_id,
	round(mileage_limit*7,0) as mileage_included_weekly,
	round(mileage_excess,2) as mileage_excess_price,
	round(rate_base_value*7,0) as rent_weekly,
	rp.short_name supplier_name,
    s.name vehicle_segment_name,
	vc.name vehicle_code,
    vc.category vehicle_category_letter,
    vc.vehicle_category_type vehicle_body_letter,
    fv.vehicle_transmission,
	IFNULL(vc.transmission, fv.vehicle_transmission) vehicle_transmission_letter,
	CASE 
        WHEN IFNULL(vc.fuel, fv.vehicle_fuel_type_code) = 'D' THEN 'diesel'
        WHEN IFNULL(vc.fuel, fv.vehicle_fuel_type_code) IN ('E','F') THEN 'electric'
        WHEN IFNULL(vc.fuel, fv.vehicle_fuel_type_code) = 'H' THEN 'hybrid'
        WHEN IFNULL(vc.fuel, fv.vehicle_fuel_type_code) = 'L' THEN 'lpg'
        WHEN IFNULL(vc.fuel, fv.vehicle_fuel_type_code) = 'G' THEN 'gasoline'
        ELSE NULL 
    END vehicle_fuel_type,
    IFNULL(vc.fuel, fv.vehicle_fuel_type_code) vehicle_fuel_letter,
    rc.rental_contract as vehicle_contract_type
FROM {{ ref('stg_odoo__rental_contracts') }} rc
left join {{ ref('stg_odoo__rate_bases') }} rb on rc.rate_base_id = rb.id
left join {{ ref('dim_contacts') }} rp on rb.partner_id = rp.contact_id
left join {{ ref('stg_odoo__vehicle_categories') }} vc on rb.vehicle_category_id = vc.id
left join {{ ref('stg_odoo__segments') }} s on vc.segment_id = s.id
left join {{ ref('dim_accounting_analytic_accounts') }} aaa on rc.billing_account_id = aaa.account_id
left join {{ ref('dim_vehicles') }} fv on rc.vehicle_id = fv.vehicle_id
left join {{ ref('dim_users') }} u on aaa.account_owner_contact_id = u.contact_id
WHERE rc.active is true
ORDER BY start_date DESC