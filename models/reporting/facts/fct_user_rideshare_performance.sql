
SELECT * FROM {{ ref('base_uber_performance') }}
UNION ALL
SELECT * FROM {{ ref('base_bolt_performance') }}
ORDER BY date DESC