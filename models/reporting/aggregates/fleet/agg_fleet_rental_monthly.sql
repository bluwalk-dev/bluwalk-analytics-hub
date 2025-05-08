SELECT
    year_month,
    COUNT(DISTINCT date) AS nr_days,
    SUM(rental_days) AS rental_days,
    ROUND(SUM(rental_days) / COUNT(DISTINCT date),2) as fleet_size
FROM {{ ref('agg_fleet_rental_daily') }}
GROUP BY year_month
ORDER BY year_month DESC