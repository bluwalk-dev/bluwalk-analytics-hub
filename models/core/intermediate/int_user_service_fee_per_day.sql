WITH 

vehicle_usage AS (
    SELECT DISTINCT
        CAST(a.dropoff_timestamp AS DATE) date,
        a.user_id,
        a.vehicle_contract_key,
        b.vehicle_contract_type,
        b.vehicle_plate,
        b.start_date,
        b.service_fee
    FROM bluwalk-analytics-hub.core.core_rideshare_user_trips a
    LEFT JOIN bluwalk-analytics-hub.core.core_driver_vehicle_contracts b ON a.vehicle_contract_key = b.vehicle_contract_key
    WHERE
        a.user_id is not null AND 
        a.vehicle_contract_key is not null
)

SELECT 
    b.date, 
    b.user_id,
    c.contact_id,
    b.service_fee,
    b.vehicle_contract_type,
    b.vehicle_plate
FROM (
    SELECT
        date, 
        user_id, 
        max(start_date) start_date
    FROM vehicle_usage
    GROUP BY 
        date, user_id
) a
LEFT JOIN vehicle_usage b ON
    a.date = b.date AND 
    a.user_id = b.user_id AND
    a.start_date = b.start_date
LEFT JOIN bluwalk-analytics-hub.core.core_users c ON a.user_id = c.user_id

ORDER BY
    date DESC, 
    user_id DESC