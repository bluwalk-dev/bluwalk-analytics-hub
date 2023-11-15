{{ config(materialized='table') }}

SELECT 
    *
FROM (
    SELECT * FROM {{ ref('base_uber_trips') }}
    UNION ALL
    SELECT * FROM {{ ref('base_bolt_trips') }}
    UNION ALL
    SELECT * FROM {{ ref('base_freenow_trips') }}
) x
ORDER BY request_local_time DESC