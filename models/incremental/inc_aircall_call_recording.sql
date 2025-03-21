SELECT
    a.db_id AS id,
    ended_at
FROM {{ ref('int_aircall_call_ended') }} a 
LEFT JOIN {{ ref('stg_aircall__call_recordings') }} b 
    ON a.db_id = b.id
WHERE a.recording_url IS NOT NULL 
    AND b.url IS NULL
