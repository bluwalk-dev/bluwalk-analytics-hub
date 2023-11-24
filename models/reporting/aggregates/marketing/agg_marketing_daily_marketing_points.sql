SELECT
    b.date,
    b.year_week,
    b.year_month,
    SUM(a.marketing_point_score) marketing_points
FROM {{ ref("fct_deals") }} a
LEFT JOIN {{ ref("util_calendar") }} b ON a.create_date = b.date
WHERE a.create_date <= current_date
GROUP BY b.date, b.year_week, b.year_month
ORDER BY b.date DESC