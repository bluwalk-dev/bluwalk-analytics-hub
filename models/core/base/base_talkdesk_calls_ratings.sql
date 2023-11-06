SELECT 

    'talkdesk' feedback_system,
    CAST(a.call_start_time AS DATETIME) date_time,
    CAST(c.id AS INT64) ticket_id,
    g.user_id,
    g.contact_id,
    f.employee_user_id agent_employee_id,
    f.employee_short_name agent_name,
    b.exit original_score,
    CAST(b.exit AS NUMERIC) normalized_score,
    '' feedback_comment

FROM {{ ref('stg_talkdesk__calls') }} a 
LEFT JOIN {{ ref('stg_talkdesk__flows') }} b on a.interaction_id = b.interaction_id 
LEFT JOIN {{ ref('int_zendesk_tickets_last_state') }} c on a.interaction_id = c.external_id
LEFT JOIN {{ ref('int_zendesk_users_last_state') }} d on cast(c.requester_id as INT64) = CAST(d.id AS INT64)
LEFT JOIN {{ ref('base_talkdesk_agents') }} e on a.agent_name = e.agent_name
LEFT JOIN {{ ref("dim_employees__version2") }} f ON e.agent_email = f.employee_email
LEFT JOIN {{ ref('dim_users__version2') }} g ON CAST(d.external_id AS INT64) = g.user_id
WHERE 
    b.flow_name ="Bluwalk 29/4" AND 
    b.step_name like "survey%" AND 
    b.exit != "timeout" AND 
    b.exit != "room_finished" AND 
    a.call_start_time >= '2023-01-01' AND 
    b.exit not like "error" AND 
    b.exit not like "invalid"