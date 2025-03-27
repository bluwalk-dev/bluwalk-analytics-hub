WITH recent_vehicle_contracts AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY vehicle_plate
               ORDER BY start_date DESC
           ) AS rn
    FROM {{ ref('dim_vehicle_contracts') }}
)
SELECT
    upa.partner_key,
    upa.sales_partner_id,
    upa.partner_name,
    '' partner_login_id,
    '' partner_login_location,
    upa.contact_id,
    u.user_id,
    upa.partner_account_uuid,
    ta.driver_name,
    ta.vehicle_plate,
    z.vehicle_contract_type,
    z.vehicle_contract_key,
    ta.request_timestamp,
    ta.request_local_time,
    ta.address_pickup,
    ta.dropoff_timestamp,
    ta.dropoff_local_time,
    ta.address_dropoff,
    NULL trip_distance
from {{ ref('stg_freenow__trips') }} ta
LEFT JOIN {{ ref('dim_partners_accounts') }} upa on ta.partner_account_uuid = upa.partner_account_uuid
LEFT JOIN {{ ref('dim_users') }} u on u.contact_id = upa.contact_id
LEFT JOIN recent_vehicle_contracts z ON ta.vehicle_plate = z.vehicle_plate AND z.rn = 1 AND ta.request_timestamp BETWEEN CAST(z.start_date AS TIMESTAMP) AND CAST(IFNULL(z.end_date, current_date) as TIMESTAMP)
WHERE 
    ta.trip_status = 'ACCOMPLISHED'
ORDER BY request_timestamp DESC