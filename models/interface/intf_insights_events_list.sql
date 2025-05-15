{{ 
  config(
    materialized='table',
    tags=['insights']
  ) 
}}

SELECT 
    location_id,
    event_title,
    venue_name,
    event_start_date,
    event_end_date,
    event_category,
    event_labels,
    event_phq_attendance expected_attendance,
    event_navigation_link
FROM {{ ref("base_predict_hq_events") }}
WHERE 
    event_phq_attendance > 1000 AND
    event_end_date >= current_date()
ORDER BY event_start_date ASC