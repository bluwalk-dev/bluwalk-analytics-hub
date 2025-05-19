WITH
calendar_agents AS (
    SELECT
        c.date,
        a.user_name as agent_name,
        a.email as agent_email,
        a.hubspot_team_name as agent_team
    FROM {{ ref("util_calendar") }} c
    CROSS JOIN {{ ref("base_hubspot_users") }} a
    WHERE hubspot_team_name = 'Insurance'
),
calls AS (
    SELECT
        date,
        agent_name,
        agent_email,
        SUM(total_inbound) as total_inbound,
        SUM(total_outbound) as total_outbound
    FROM {{ ref("agg_operations_daily_team_agent_calls") }} a
    LEFT JOIN {{ ref("base_hubspot_users") }} b ON a.agent_email = b.email
    WHERE hubspot_team_name = 'Insurance'
    GROUP BY
        date,
        agent_name,
        agent_email
),
email_activity AS (
    SELECT
        CAST(create_local_time AS DATE) as date,
        agent_name,
        agent_email,
        SUM(CASE WHEN email_direction = 'inbound' THEN 1 ELSE 0 END) AS inbound_emails,
        SUM(CASE WHEN email_direction = 'outbound' THEN 1 ELSE 0 END) AS outbound_emails
    FROM {{ ref("base_hubspot_emails") }}
    WHERE agent_team = 'Insurance'
    GROUP BY
        date,
        agent_name,
        agent_email
)

SELECT
    a.date,
    a.agent_name,
    a.agent_email,
    COALESCE(b.total_inbound, 0) as calls_inbound,
    COALESCE(b.total_outbound, 0) as calls_outbound,
    COALESCE(c.inbound_emails, 0) as email_inbound,
    COALESCE(c.outbound_emails, 0) as email_outbound
FROM calendar_agents a
LEFT JOIN calls b ON a.date = b.date and a.agent_email = b.agent_email
LEFT JOIN email_activity c ON a.date = c.date and a.agent_email = c.agent_email
WHERE 
    a.date BETWEEN '2023-01-01' AND CURRENT_DATE()
ORDER BY date DESC