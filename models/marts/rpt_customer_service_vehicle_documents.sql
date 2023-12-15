SELECT
    c.user_vat,
    c.user_name,
    'Bolt' partner,
    b.vehicle_contract_type,
    a.*
FROM {{ ref("base_bolt_documents") }} a
LEFT JOIN {{ ref("dim_vehicle_contracts") }} b ON a.vehicle_plate = b.vehicle_plate
LEFT JOIN {{ ref("dim_users") }} c ON b.contact_id = c.contact_id
WHERE b.end_date IS NULL AND b.contact_id IS NOT NULL
ORDER BY expires_timestamp ASC