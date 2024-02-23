{{ config(materialized='table') }}

SELECT 
    location_id,
    event_title,
    venue_name,
    event_start_date,
    event_end_date,
    event_category,
    event_labels,
    expected_attendance,
    event_navigation_link
FROM {{ ref("base_predict_hq_events") }}
WHERE event_phq_attendance > 1000
ORDER BY event_start_date ASC