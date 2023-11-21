SELECT
    a.event_id,
    a.original_timestamp,
    a.user_id,
    b.contact_id,
    a.nps_response,
    a.nps_response_comments,
    CASE 
        WHEN a.nps_response >= 9 THEN 'promoter'
        WHEN a.nps_response <= 6 THEN 'detractor'
        ELSE 'passive'
    END nps_response_classification
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY loaded_at DESC) AS __row_number 
    FROM {{ ref('stg_segment__nps_response_submitted') }}
) a
LEFT JOIN {{ ref('dim_users') }} b ON a.user_id = b.user_id
WHERE __row_number = 1
ORDER BY a.original_timestamp DESC