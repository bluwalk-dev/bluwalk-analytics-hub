SELECT
    b.vehicle_plate, 
    d.user_email,
    d.user_id
FROM {{ ref("dim_vehicle_insurances") }} a
LEFT JOIN {{ ref("dim_vehicles") }} b ON a.vehicle_id = b.vehicle_id
LEFT JOIN {{ ref("dim_users") }} d on b.current_driver_contract_id = d.contact_id
where 
	a.end_date = date_add(current_date(), INTERVAL 30 day) AND
	a.state = 'active'