SELECT
    contact_id,
    statement,
    SUM(nr_trips) AS nr_trips,
    SUM(trips_received) AS nr_trips_received,
    CASE WHEN SUM(trips_received) > 0 THEN  ROUND((SUM(nr_trips) / SUM(trips_received)) * 100, 2) ELSE NULL END AS acceptance_rate
FROM
    (SELECT
        a.contact_id,
        a.nr_trips,
        year_week statement,
        CASE WHEN a.acceptance_rate != 0 THEN a.nr_trips / a.acceptance_rate ELSE NULL END AS trips_received
    FROM {{ ref('fct_user_rideshare_performance') }} a
    LEFT JOIN {{ ref('util_calendar') }} b ON a.date = b.date
    WHERE a.date >= '2023-01-01')
GROUP BY contact_id, statement