SELECT 
  destination_code_iata,
  CAST(flight_scheduled_in AS DATE) date,
  EXTRACT(HOUR FROM flight_scheduled_in) time,
  SUM(IFNULL(flight_seats,0)) expected_passengers
FROM {{ ref("base_flight_aware_scheduled_arrivals") }}
WHERE flight_scheduled_in IS NOT NULL
GROUP BY 
    1, 2, 3
ORDER BY
  2 ASC,
  3 ASC