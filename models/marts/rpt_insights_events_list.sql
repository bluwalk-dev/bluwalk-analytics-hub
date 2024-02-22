SELECT 
    location_id,
    event_title,
    venue_name,
    event_start_date,
    event_phq_attendance,
    event_location
FROM {{ ref("base_predict_hq_events") }}
WHERE event_phq_attendance > 1000