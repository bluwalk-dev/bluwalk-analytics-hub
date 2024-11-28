WITH call_metrics AS (
    SELECT
        CAST(started_local AS DATE) AS date,
        brand,
        team,
        COUNTIF(direction = 'inbound') AS total_inbound,
        COUNTIF(direction = 'inbound' AND valid_call = TRUE) AS total_valid_inbound,
        COUNTIF(direction = 'inbound' AND valid_call = TRUE AND missed_call_reason IS NOT NULL) AS missed_inbound
    FROM {{ ref('base_aircall_calls') }}
    GROUP BY date, team, brand
)

SELECT
    a.date,
    b.team,
    b.brand,
    COALESCE(b.total_inbound, 0) AS total_inbound,
    COALESCE(b.total_valid_inbound, 0) AS total_valid_inbound,
    COALESCE(b.missed_inbound, 0) AS missed_inbound
FROM {{ ref('util_calendar') }} a
LEFT JOIN call_metrics b ON a.date = b.date
WHERE a.date <= CURRENT_DATE()
ORDER BY a.date DESC, b.team DESC