SELECT 
    insurance_policy_number, 
    insurer_name, 
    vehicle_plate, 
    contact_full_name
FROM bluwalk-analytics-hub.core.core_insurance_policies a
LEFT JOIN {{ ref('dim_vehicles') }} b on a.insurance_vehicle_id = b.vehicle_id
LEFT JOIN {{ ref('dim_contacts') }} c on a.insurance_contact_id = c.contact_id
WHERE 
  insurance_renewal_date > date_sub(current_date(), INTERVAL 60 DAY) and
  insurance_state = 'active' and
  insurance_type_line = 'auto' and
  vehicle_plate is not null