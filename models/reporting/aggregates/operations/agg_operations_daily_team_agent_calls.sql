WITH call_metrics AS (
    SELECT
        CAST(started_local AS DATE) AS date,
        team,
        agent_name,
        agent_email,
        COUNTIF(direction = 'inbound') AS total_inbound,
        COUNTIF(direction = 'outbound') AS total_outbound
    FROM {{ ref('base_aircall_calls') }}
    WHERE user_id IS NOT NULL
    GROUP BY date, agent_name, agent_email, team
)

SELECT
    a.date,
    team,
    agent_name,
    agent_email,
    COALESCE(b.total_inbound, 0) AS total_inbound,
    COALESCE(b.total_outbound, 0) AS total_outbound
FROM {{ ref('util_calendar') }} a
LEFT JOIN call_metrics b ON a.date = b.date
WHERE a.date <= CURRENT_DATE()
ORDER BY a.date DESC