SELECT
    upa.partner_key,
    upa.sales_partner_id,
    upa.partner_name,
    ta.partner_login_id,
    upa.contact_id,
    u.user_id,
    ta.partner_account_uuid,
    ta.driver_name,
    ta.vehicle_plate,
    z.vehicle_contract_type,
    z.vehicle_contract_id,
    ta.request_timestamp,
    ta.request_local_time,
    ta.address_pickup,
    ta.dropoff_timestamp,
    ta.dropoff_local_time,
    ta.address_dropoff,
    ta.trip_distance
from {{ ref('stg_uber__trips') }} ta
LEFT JOIN {{ ref('dim_partners_accounts') }} upa on ta.partner_account_uuid = upa.partner_account_uuid
LEFT JOIN {{ ref('dim_users') }} u on u.contact_id = upa.contact_id
LEFT JOIN {{ ref('dim_vehicle_contracts') }} z ON ta.vehicle_plate = z.vehicle_plate
WHERE
    (ta.request_timestamp BETWEEN CAST(z.start_date AS TIMESTAMP) AND CAST(IFNULL(z.end_date, current_date) as TIMESTAMP) OR z.vehicle_plate IS NULL) AND
    ta.trip_status = 'completed'
ORDER BY request_timestamp DESC