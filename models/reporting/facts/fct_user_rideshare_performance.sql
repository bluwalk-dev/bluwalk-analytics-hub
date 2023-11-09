SELECT * FROM (
    SELECT * FROM {{ ref('base_uber_performance') }}
    UNION ALL
    SELECT * FROM {{ ref('base_bolt_performance') }}
)
WHERE (nr_trips > 0 AND online_minutes > 0)
ORDER BY date DESC