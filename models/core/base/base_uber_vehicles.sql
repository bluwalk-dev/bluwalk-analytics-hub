SELECT * EXCEPT(__row_number) 
FROM
    (SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY vehicle_plate, vehicle_owner_id 
            ORDER BY load_timestamp DESC
        ) AS __row_number
    FROM {{ ref("stg_uber__vehicles") }} 
    )
WHERE __row_number = 1