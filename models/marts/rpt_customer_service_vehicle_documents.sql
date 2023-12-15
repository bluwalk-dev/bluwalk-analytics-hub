SELECT
    'Bolt' partner,
    a.*,
    b.contact_id,
    b.vehicle_contract_type
FROM {{ ref("base_bolt_documents") }} a
LEFT JOIN {{ ref("dim_vehicle_contracts") }} b ON a.vehicle_plate = b.vehicle_plate
WHERE b.end_date IS NULL AND contact_id IS NOT NULL
ORDER BY expires_timestamp ASC