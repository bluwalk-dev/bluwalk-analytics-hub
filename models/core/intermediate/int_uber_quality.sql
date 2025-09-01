SELECT
    date,
    partner_account_uuid,
    first_name,
    last_name,
    SUM(nr_trips) nr_trips,
    SUM(acceptance_rate * nr_trips) / NULLIF(SUM(nr_trips), 0) acceptance_rate,
    SUM(cancellation_rate * nr_trips) / NULLIF(SUM(nr_trips), 0) cancellation_rate,
    SUM(conclusion_rate * nr_trips) / NULLIF(SUM(nr_trips), 0) conclusion_rate,
    AVG(rating_4_weeks) rating_4_weeks,
    AVG(rating_last_500_trips) rating_last_500_trips
FROM bluwalk-analytics-hub.staging.stg_uber_driver_quality
GROUP BY date, partner_account_uuid, first_name, last_name
ORDER BY date DESC