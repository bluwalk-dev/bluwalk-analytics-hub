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
        CAST(end_time AS DATE) date,
        count(*) nr_inbound_calls
    FROM {{ ref('fct_calls') }}
    WHERE direction = 'inbound'  AND internal_line_name = 'Customer Service'
    GROUP BY date
) b ON a.date = b.date
LEFT JOIN (
    SELECT
        CAST(end_time AS DATE) date,
        count(*) nr_missed_calls
    FROM {{ ref('fct_calls') }}
    WHERE direction = 'inbound' AND outcome = 'missed' AND internal_line_name = 'Customer Service'
    GROUP BY date
) c ON a.date = c.date
WHERE a.year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
GROUP BY a.year_month
ORDER BY a.year_month DESC