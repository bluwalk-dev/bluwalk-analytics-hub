SELECT
    a.call_system,
    a.call_uuid,
    a.direction,
    a.outcome,
    a.start_time, 
    a.end_time, 
    a.contact_phone_nr,
    c.contact_id,
    c.user_id,
    a.duration,
    a.internal_line_nr, 
    a.internal_line_name,
    a.agent_email,
    b.employee_team agent_team,
    a.recording_link
FROM (
    SELECT * FROM {{ ref('base_talkdesk_calls') }}
    UNION ALL 
    SELECT * FROM {{ ref('base_aircall_calls') }}
) a
LEFT JOIN {{ ref('dim_employees') }} b ON a.agent_email = b.employee_email
LEFT JOIN {{ ref('base_hubspot_contacts') }} c ON a.contact_phone_nr = c.contact_phone_nr
ORDER BY a.start_time DESC