{{ config(materialized='table') }}

WITH hours AS (
  SELECT hour
  FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour -- Generates numbers from 0 to 23 for hours
),
dates AS (
  SELECT 
    destination_location_id,
    CONCAT(
      destination_airport_name, ' (',
      destination_code_iata, ')'
    ) AS airport_name,
    destination_navigation_link airport_url,
    CAST(flight_scheduled_in AS DATE) AS date
  FROM {{ ref("base_flight_aware_scheduled_arrivals") }}
  WHERE flight_scheduled_in IS NOT NULL
  GROUP BY date, airport_name, destination_location_id, destination_navigation_link
),
cross_join_dates_hours AS (
  SELECT 
    d.destination_location_id,
    d.airport_name,
    d.airport_url,
    d.date,
    h.hour
  FROM dates d
  CROSS JOIN hours h
),
expected_passengers AS (
  SELECT 
    destination_location_id,
    CONCAT(
      destination_airport_name, ' (',
      destination_code_iata, ')'
    ) AS airport_name,
    CAST(flight_scheduled_in AS DATE) AS date,
    EXTRACT(HOUR FROM flight_scheduled_in) AS hour,
    SUM(IFNULL(flight_seats, 0)) AS expected_passengers
  FROM {{ ref("base_flight_aware_scheduled_arrivals") }}
  WHERE flight_scheduled_in IS NOT NULL
  GROUP BY destination_location_id, airport_name, date, hour
)
SELECT 
  c.destination_location_id,
  c.airport_name,
  c.airport_url,
  c.date,
  c.hour,
  COALESCE(e.expected_passengers, 0) AS expected_passengers
FROM cross_join_dates_hours c
LEFT JOIN expected_passengers e ON c.date = e.date AND c.hour = e.hour AND c.airport_name = e.airport_name
ORDER BY c.date ASC, c.hour ASC