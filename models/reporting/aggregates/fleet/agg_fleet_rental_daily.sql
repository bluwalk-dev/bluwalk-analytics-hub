SELECT
    a.date,
    b.year_month,
    b.year_week,
    ROUND(SUM(total_days),0) rental_days
FROM {{ ref('int_fleet_rentals_per_day_list') }} a
LEFT JOIN {{ ref('util_calendar') }} b ON a.date = b.date
WHERE a.date <= 
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) = 1 
      THEN DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK), INTERVAL 7 DAY)
    ELSE DATE_TRUNC(CURRENT_DATE(), WEEK)
  END
GROUP BY date, year_month, year_week
ORDER BY date DESC