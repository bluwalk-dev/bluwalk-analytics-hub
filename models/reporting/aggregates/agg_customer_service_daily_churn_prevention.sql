SELECT
    c.date,
    if(g.success is null, 0, g.success) retention_success, 
    (if(g.success is null, 0, g.success) + if(b.no_success is null, 0, b.no_success)) retention_attempts
FROM {{ ref('util_calendar') }} c
LEFT JOIN (
    SELECT contacted_at, count(*) success
    FROM {{ ref('stg_google_sheets__churn_prevention') }}
    WHERE retained = 'Sim'
    GROUP BY contacted_at) g ON c.date = g.contacted_at
LEFT JOIN (
    SELECT contacted_at, count(*) no_success
    FROM {{ ref('stg_google_sheets__churn_prevention') }}
    WHERE retained = 'Não'
    GROUP BY contacted_at) b ON  c.date = b.contacted_at
WHERE c.date <= current_date()
ORDER BY c.date DESC