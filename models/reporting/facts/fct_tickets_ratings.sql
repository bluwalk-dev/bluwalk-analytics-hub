SELECT
    feedback_system,
    brand,
    date_time,
    ticket_id,
    user_id,
    contact_id,
    agent_name,
    agent_team,
    original_score,
    normalized_score,
    feedback_comment
FROM (
    SELECT * FROM {{ ref('base_odoo_tickets_ratings') }}
    UNION ALL
    SELECT * FROM {{ ref('base_zendesk_tickets_ratings') }}
    UNION ALL 
    SELECT * FROM {{ ref('base_hubspot_tickets_ratings') }}
    UNION ALL 
    SELECT * FROM {{ ref('base_talkdesk_calls_ratings') }}
)
ORDER BY date_time DESC
