SELECT
    c.year_month,
    SUM(IFNULL(g.success, 0)) retention_success, 
    SUM(IFNULL(g.success, 0) + IFNULL(b.no_success, 0)) retention_attempts
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
WHERE c.year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
GROUP BY c.year_month
ORDER BY c.year_month DESC