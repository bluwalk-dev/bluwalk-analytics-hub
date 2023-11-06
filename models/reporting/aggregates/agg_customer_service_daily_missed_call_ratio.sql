SELECT 
    a.date,
    COALESCE(c.nr_missed_calls,0) nr_missed_calls,
    COALESCE(b.nr_inbound_calls,0) nr_inbound_calls,
    CASE
        WHEN COALESCE(b.nr_inbound_calls, 0) > 0 THEN
            c.nr_missed_calls::FLOAT / NULLIF(b.nr_inbound_calls, 0)
        ELSE
            NULL
    END AS missed_call_ratio
FROM {{ ref('util_calendar') }} a
LEFT JOIN (
    SELECT
        CAST(callEndTime AS DATE) date,
        count(*) nr_inbound_calls
    FROM {{ ref('fct_calls') }}
    WHERE direction = 'inbound'  AND agent_team = 'Customer Service'
    GROUP BY date
) b ON a.date = b.date
LEFT JOIN (
    SELECT
        CAST(callEndTime AS DATE) date,
        count(*) nr_missed_calls
    FROM {{ ref('fct_calls') }}
    WHERE direction = 'inbound' AND outcome = 'missed' AND agent_team = 'Customer Service'
    GROUP BY date
) c ON a.date = c.date