WITH all_inbound_calls AS (
    SELECT 
        created_at, 
        answered_at,
        team
    FROM {{ ref('base_calls_inbound_team') }}
    WHERE 
        team = 'Service' AND
        duration > 10

    UNION ALL

    -- No Agent Available Calls
    SELECT
        created_at, 
        NULL answered_at,
        team
    FROM {{ ref('int_aircall_call_no_agents_available') }}
    WHERE team = 'Service'
)

SELECT
    a.year_month,
    COALESCE(SUM(c.nr_missed_calls),0) nr_missed_calls,
    COALESCE(SUM(b.nr_inbound_calls),0) nr_inbound_calls,
    CASE
        WHEN COALESCE(SUM(b.nr_inbound_calls), 0) > 0 THEN
            ROUND(SUM(c.nr_missed_calls) / NULLIF(SUM(b.nr_inbound_calls), 0),4)
        ELSE
            NULL
    END AS missed_call_ratio
FROM {{ ref('util_calendar') }} a
LEFT JOIN (
    SELECT
        CAST(created_at AS DATE) date,
        count(*) nr_inbound_calls
    FROM all_inbound_calls
    GROUP BY date
) b ON a.date = b.date
LEFT JOIN ( 
    SELECT
        CAST(created_at AS DATE) date,
        count(*) nr_missed_calls
    FROM all_inbound_calls
    WHERE answered_at IS NULL
    GROUP BY date
) c ON a.date = c.date
WHERE 
    a.year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
GROUP BY a.year_month
ORDER BY a.year_month DESC