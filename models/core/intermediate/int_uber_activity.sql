SELECT
    date,
    partner_account_uuid,
    first_name,
    last_name,
    SUM(nr_trips) nr_trips,
    SUM(online_minutes) online_minutes,
    SUM(trip_minutes) trip_minutes
FROM bluwalk-analytics-hub.staging.stg_uber_driver_activity
GROUP BY date, partner_account_uuid, first_name, last_name
ORDER BY date DESC