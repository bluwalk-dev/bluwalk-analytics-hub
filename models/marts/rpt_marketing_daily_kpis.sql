SELECT 
    a.date,
    a.year_week,
    a.year_month,
    IFNULL(b.marketing_points,0) marketing_points
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('agg_marketing_daily_marketing_points') }} b ON a.date = b.date
WHERE a.date <= current_date
ORDER BY a.date DESC