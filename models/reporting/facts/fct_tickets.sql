select
    ticket_system,
    brand,
    ticket_id,
    source,
    agent_name,
    agent_team,
    contact_id,
    user_id,
    user_location,
    subject,
    priority,
    stage,
    category,
    create_date,
    close_date,
    resolution_time,
    first_reply_time
from {{ ref("base_hubspot_tickets") }}
order by create_date DESC
