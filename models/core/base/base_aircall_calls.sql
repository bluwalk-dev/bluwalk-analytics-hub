SELECT DISTINCT
    cc.call_uuid,
    'aircall' call_system,
    cc.direction direction,
    IF(cc.direction = 'Inbound', 
        IF(ca.id IS NOT NULL, "completed",
        IF(cra.id IS NOT NULL, "missed", "abandoned")
        ), IF(ca.id IS NOT NULL, "completed", 'missed'
        )
    ) as outcome,
    DATETIME(TIMESTAMP_SECONDS(cc.started_at), 'Europe/Lisbon') start_time, 
    DATETIME(TIMESTAMP_SECONDS(ce.ended_at), 'Europe/Lisbon') end_time, 
    cc.user_id contact_phone_nr,
    ce.duration duration,
    REPLACE(cc.number_digits, ' ','') internal_line_nr, 
    ce.number_name internal_line_name,
    IFNULL(cc.user_email, ca.user_email) agent_email,
    ce.asset recording_link
FROM {{ ref('int_aircall_call_created_last_state') }} cc
LEFT JOIN {{ ref('int_aircall_call_answered_last_state') }} ca ON cc.call_uuid = ca.call_uuid
LEFT JOIN {{ ref('int_aircall_call_ended_last_state') }} ce ON cc.call_uuid = ce.call_uuid
LEFT JOIN {{ ref('int_aircall_call_ringing_on_agent_last_state') }} cra ON cc.call_uuid = cra.call_uuid