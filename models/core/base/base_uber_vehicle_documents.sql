WITH last_version_documents AS (
    SELECT * EXCEPT(__row_number) 
    FROM
        (SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY vehicle_id, vehicle_owner_id, document_type_id
                ORDER BY load_timestamp DESC
            ) AS __row_number
        FROM {{ ref("stg_uber__vehicle_documents") }} 
        )
    WHERE __row_number = 1
)

SELECT 
    a.vehicle_owner_id, 
    b.vehicle_plate,
    a.document_global_type_name,
    a.document_type_name,
    a.document_status,
    a.document_expires_at
FROM last_version_documents a
LEFT JOIN {{ ref("base_uber_vehicles") }} b ON a.vehicle_id = b.vehicle_id