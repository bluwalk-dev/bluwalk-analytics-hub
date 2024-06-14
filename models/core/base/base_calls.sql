SELECT
    a.call_uuid,
    a.direction,
    a.created_at,
    b.ended_at,
    b.duration,
    b.number_name,
    b.missed_call_reason,
    b.recording_url
FROM {{ ref('int_aircall_call_created') }} a
LEFT JOIN {{ ref('int_aircall_call_ended') }} b ON a.call_uuid = b.call_uuid
ORDER BY created_at DESC
