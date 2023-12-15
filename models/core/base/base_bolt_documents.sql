SELECT 
    b.vehicle_plate,
    b.car_group_name,
    a.document_id,
    a.expires_timestamp,
    a.status,
    a.type,
    a.isExpired,
    a.expiresInDays,
    a.uploaded_document_id,
    a.uploaded_expires_timestamp,
    a.uploaded_status,
    a.uploaded_type,
    a.uploaded_isExpired,
    a.uploaded_expiresInDays
FROM
    (SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY document_id
            ORDER BY extraction_timestamp DESC
        ) AS __row_number
    FROM {{ ref("stg_bolt__vehicle_documents") }} 
    ) a
LEFT JOIN {{ ref("base_bolt_vehicles") }} b ON a.carId = b.vehicle_id
WHERE __row_number = 1