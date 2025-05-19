SELECT
    year_month,
    COUNT(DISTINCT date) AS nr_days,
    
    -- Old Fields (not to use)
    SUM(rental_days) AS rental_days,
    ROUND(SUM(rental_days) / COUNT(DISTINCT date),2) as fleet_size,

    -- New Fields
    ROUND(SUM(driver_rental_days) / COUNT(DISTINCT date), 2) as driver_rentals,
    SUM(driver_rental_days) AS driver_rental_days,
    ROUND(SUM(company_rental_days) / COUNT(DISTINCT date), 2) as company_rentals,
    SUM(company_rental_days) AS company_rental_days,
    ROUND(SUM(rental_days) / COUNT(DISTINCT date),2) as total_rentals,
    SUM(rental_days) AS total_rental_days,
FROM {{ ref('agg_fleet_rental_daily') }}
GROUP BY year_month
ORDER BY year_month DESC