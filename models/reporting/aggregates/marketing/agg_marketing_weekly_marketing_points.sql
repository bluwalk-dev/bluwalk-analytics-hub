SELECT
    b.year_week,
    c.start_date,
    c.end_date,
    SUM(marketing_point_score) marketing_points
FROM {{ ref("fct_deals") }} a
LEFT JOIN {{ ref("util_calendar") }} b ON CAST(a.create_date AS DATE) = b.date
LEFT JOIN {{ ref("util_week_intervals") }} c ON b.year_week = c.year_week
GROUP BY b.year_week, c.start_date, c.end_date
ORDER BY b.year_week DESC