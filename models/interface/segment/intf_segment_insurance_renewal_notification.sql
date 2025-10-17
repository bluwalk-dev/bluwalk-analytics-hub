SELECT
    b.vehicle_plate, 
    d.user_email,
    d.user_id
FROM bluwalk-analytics-hub.core.core_insurance_policies a
LEFT JOIN {{ ref("dim_vehicles") }} b ON a.insurance_vehicle_id = b.vehicle_id
LEFT JOIN {{ ref("dim_users") }} d on b.current_driver_contract_id = d.contact_id
where 
	a.insurance_renewal_date = date_add(current_date(), INTERVAL 30 day) AND
	a.insurance_state = 'active'