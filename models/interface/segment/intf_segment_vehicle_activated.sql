WITH 
    trip_data AS (
        SELECT
            user_id,
            vehicle_plate,
            MIN(request_timestamp) first_trip_ts
        FROM {{ ref("fct_user_rideshare_trips") }}
        WHERE request_timestamp > DATE_SUB(current_timestamp(), INTERVAL 15 DAY)
        GROUP BY
            vehicle_plate,
            user_id
    ),

    hubspot AS (
        SELECT
            a.user_id,
            CASE
                WHEN a.deal_pipeline_name = 'Vehicle : Rent : Drivers' THEN 'Drivfit'
                WHEN a.deal_pipeline_name = 'Vehicle : Personal Vehicle' THEN 'Personal Car'
                ELSE NULL
            END partner_name,
            a.vehicle_plate
        FROM  {{ ref("fct_deals") }} a
        WHERE
            a.deal_pipeline_name IN ('Vehicle : Rent : Drivers', 'Vehicle : Personal Vehicle') AND
            a.is_closed = FALSE AND
            a.vehicle_plate IS NOT NULL
    )

SELECT
    a.user_id,
    a.partner_name,
    a.vehicle_plate,
    b.first_trip_ts
FROM hubspot a
LEFT JOIN trip_data b ON
    a.user_id = b.user_id AND
    b.vehicle_plate = a.vehicle_plate
WHERE
    b.user_id IS NOT NULL