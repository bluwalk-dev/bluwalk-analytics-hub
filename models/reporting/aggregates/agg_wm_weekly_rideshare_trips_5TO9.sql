SELECT
    contact_id,
    statement,
    LEAST(SUM(between_5_and_9),7) AS nr_trips_5TO9
FROM (
    SELECT
        a.contact_id,
        b.year_week statement,
        DATE(a.request_local_time) AS request_date,
        CASE WHEN EXTRACT(HOUR FROM a.request_local_time) >= 5 AND EXTRACT(HOUR FROM a.request_local_time) <= 8 THEN 1 ELSE 0 END AS between_5_and_9,
        ROW_NUMBER() OVER (PARTITION BY a.contact_id, DATE(a.request_local_time) ORDER BY a.request_local_time) AS row_num
    FROM {{ ref('fct_rideshare_trips') }} a
    LEFT JOIN {{ ref('util_calendar') }} b ON DATE(a.request_local_time) = b.date
    WHERE a.request_local_time >= '2023-01-01')
WHERE row_num = 1
GROUP BY contact_id, statement