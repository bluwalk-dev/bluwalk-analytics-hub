SELECT 
    interaction_id call_uuid,
    'talkdesk' call_system,
    if(left(call_type,2) = 'in', 'inbound', 'outbound') direction,
    call_outcome outcome,
    call_start_time start_time,
    call_end_time end_time, 
    customer_phone_nr contact_phone_nr,
    duration_in_seconds duration,
    internal_line_nr,
    '' internal_line_name,
    b.agent_email,
    recording_link
FROM {{ ref('stg_talkdesk__calls') }} a
LEFT JOIN {{ ref('base_talkdesk_agents') }} b ON a.agent_name = b.agent_name