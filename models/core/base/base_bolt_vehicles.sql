SELECT
    vehicle_id,
    vehicle_model,
    vehicle_year,
    REPLACE(vehicle_reg_number,'-','') vehicle_plate,
    vehicle_color,
    vehicle_car_transport_licence_number,
    vehicle_car_transport_licence_expires,
    company_id,
    car_group_name,
    status
FROM
    (SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY vehicle_id
            ORDER BY extraction_timestamp DESC
        ) AS __row_number
    FROM {{ ref("stg_bolt__vehicle_engagements") }} 
    )
WHERE __row_number = 1