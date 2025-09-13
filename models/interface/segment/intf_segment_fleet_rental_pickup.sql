WITH 
hubspot_booking_created as (
    SELECT
        user_id,
        vehicle_rental_license_plate,
        vehicle_rental_rate,
        vehicle_rental_pickup_date,
        vehicle_rental_pickup_station,
        vehicle_rental_booking_type,
    FROM bluwalk-analytics-hub.core.core_hubspot_deals
    WHERE deal_pipeline_stage_id = '307257316'
),
latest_vehicle_pickups as (
    SELECT * FROM
        (SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY user_id 
                ORDER BY start_date DESC
            ) AS __row_number
        FROM {{ ref("dim_vehicle_contracts") }}
        WHERE 
            start_date > DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND 
            vehicle_contract_type = 'car_rental'
        )
    WHERE __row_number = 1
)

SELECT
    a.user_id
FROM hubspot_booking_created a
LEFT JOIN latest_vehicle_pickups b ON a.user_id = b.user_id
WHERE b.vehicle_contract_key IS NOT NULL