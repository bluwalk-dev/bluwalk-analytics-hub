SELECT
    upa.partner_id,
    upa.partner_name,
    upa.contact_id,
    u.user_id,
    upa.partner_account_uuid,
    ta.driver_name,
    ta.vehicle_plate,
    z.vehicle_contract_type,
    z.vehicle_contract_id,
    ta.request_timestamp,
    ta.request_local_time,
    ta.address_pickup,
    TIMESTAMP(NULL) as dropoff_timestamp,
    DATETIME(NULL) as dropoff_local_time,
    CAST(NULL AS STRING) as address_dropoff,
    NULL as trip_distance
FROM {{ ref('stg_bolt__trips') }} ta
LEFT JOIN {{ ref('stg_bolt__performance') }} dp ON ta.driver_name = dp.driver_name AND ta.request_date = dp.date
LEFT JOIN {{ ref('dim_partners_accounts') }} upa on dp.partner_account_uuid = upa.partner_account_uuid
LEFT JOIN {{ ref('dim_users') }} u on u.contact_id = upa.contact_id
LEFT JOIN {{ ref('dim_vehicle_contracts') }} z ON ta.vehicle_plate = z.vehicle_plate
WHERE 
    ta.request_timestamp < CAST(IFNULL(z.end_date, current_date) as TIMESTAMP) AND 
    ta.request_timestamp > CAST(z.start_date AS TIMESTAMP) AND
    trip_status = 'Concluída'
ORDER BY request_timestamp DESC