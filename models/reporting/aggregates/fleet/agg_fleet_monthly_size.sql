WITH daily_fleet_size AS (
    SELECT
        date,
        year_month,
        count(*) fleet_size
    FROM {{ ref('int_fleet_vehicles_per_day_list') }}
    GROUP BY date, year_month
)

SELECT
    year_month,
    ROUND(AVG(fleet_size),0) fleet_size
FROM daily_fleet_size
GROUP BY year_month
ORDER BY year_month DESC