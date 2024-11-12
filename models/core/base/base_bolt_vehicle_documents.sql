WITH last_version_documents AS (
    SELECT CAST(MAX(load_timestamp) AS DATE) last_version
    FROM {{ ref("stg_bolt__expiring_documents") }} 
)

SELECT
   REPLACE(identifier, '-', '') vehicle_plate,
   company_id,
   type_title document_name,
   entity_id,
   expiration_date
FROM {{ ref("stg_bolt__expiring_documents") }}
WHERE 
    CAST(load_timestamp AS DATE) = (SELECT last_version FROM last_version_documents) AND
    entity_type = 'car'