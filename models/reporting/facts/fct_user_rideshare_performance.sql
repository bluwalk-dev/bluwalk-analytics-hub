
SELECT * FROM {{ ref('base_uber_performance') }}
UNION ALL
SELECT * FROM {{ ref('base_bolt_performance') }}
WHERE (online_minutes > 0 AND nr_trips > 0)
ORDER BY date DESC