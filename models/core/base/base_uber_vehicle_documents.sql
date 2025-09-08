SELECT 
    a.vehicle_owner_id, 
    b.vehicle_plate,
    a.document_global_type_name,
    a.document_type_name,
    a.document_status,
    a.document_expires_at,
    c.org_name as account_name,
    c.location_name
FROM bluwalk-analytics-hub.staging.stg_uber_vehicle_documents a
LEFT JOIN bluwalk-analytics-hub.staging.stg_uber_vehicles b ON a.vehicle_id = b.vehicle_id
LEFT JOIN {{ ref("dim_partners_logins") }} c ON a.vehicle_owner_id = c.login_id