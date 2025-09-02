WITH last_version_documents AS (
    SELECT MAX(load_timestamp) last_version
    FROM bluwalk-analytics-hub.staging.stg_bolt_documents_expiring
)

SELECT
    REPLACE(a.identifier, '-', '') vehicle_plate,
    a.company_id,
    a.type_title document_name,
    a.entity_id,
    CASE 
        WHEN a.expiration_date < current_date THEN 'expired'
        ELSE 'active'
    END document_status,
    a.expiration_date,
    b.org_name as account_name,
    b.location_name
FROM bluwalk-analytics-hub.staging.stg_bolt_documents_expiring a
LEFT JOIN {{ ref("dim_partners_logins") }} b ON a.company_id = b.login_id
WHERE 
    load_timestamp = (SELECT last_version FROM last_version_documents) AND
    entity_type = 'car'