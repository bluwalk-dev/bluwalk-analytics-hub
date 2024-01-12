SELECT 
    contact_id,
    year_week statement,
    SUM(nr_worked_weekend_days) nr_worked_weekend_days
FROM (
    SELECT
        contact_id,
        request_date,
        LEAST(SUM(is_weekend_and_after_5am),1) AS nr_worked_weekend_days
    FROM (
        SELECT
            a.contact_id,
            DATE(a.request_local_time) AS request_date,
            CASE 
            WHEN EXTRACT(DAYOFWEEK FROM a.request_local_time) IN (1, 7) -- 1 = Sunday, 7 = Saturday
                AND EXTRACT(HOUR FROM a.request_local_time) >= 5 THEN 1
            ELSE 0
            END AS is_weekend_and_after_5am
        FROM {{ ref('fct_user_rideshare_trips') }} a
        WHERE a.request_local_time >= '2023-01-01')
        GROUP BY contact_id, request_date
) a
LEFT JOIN {{ ref('util_calendar') }} b ON a.request_date = b.date
GROUP BY contact_id, statement