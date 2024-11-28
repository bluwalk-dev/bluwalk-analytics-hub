SELECT 
    'zendesk' feedback_system,
    'Bluwalk' brand,
    CAST(a.created_at AS DATETIME) date_time,
    CAST(a.id AS INT64) ticket_id,
    u.user_id,
    u.contact_id,
    e.employee_short_name agent_name,
    'Customer Service' agent_team,
    CAST(satisfaction_rating_score AS STRING) original_score,
    CAST(IF(satisfaction_rating_score='good',1,0) AS NUMERIC) normalized_score,
    satisfaction_rating_comment feedback_comment
FROM {{ ref('int_zendesk_tickets_last_state') }} a
left join {{ ref('int_zendesk_users_last_state') }} b on CAST(a.assignee_id AS INT64) = CAST(b.id AS INT64)
left join {{ ref('int_zendesk_users_last_state') }} c on CAST(a.requester_id AS INT64) = CAST(c.id AS INT64)
LEFT JOIN {{ ref("dim_employees") }} e ON b.email = e.employee_email
LEFT JOIN {{ ref('dim_users') }} u ON CAST(c.external_id AS INT64) = u.user_id
WHERE satisfaction_rating_score IN ('good', 'bad')