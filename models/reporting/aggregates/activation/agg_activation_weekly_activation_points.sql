SELECT
    b.year_week,
    c.start_date,
    c.end_date,
    SUM(activation_point_score) activation_points
FROM {{ ref("fct_deals") }} a
LEFT JOIN {{ ref("util_calendar") }} b ON CAST(a.close_date AS DATE) = b.date
LEFT JOIN {{ ref("util_week_intervals") }} c ON b.year_week = c.year_week
WHERE 
    is_closed_won IS TRUE AND 
    a.close_date IS NOT NULL
GROUP BY b.year_week, c.start_date, c.end_date
ORDER BY b.year_week DESC