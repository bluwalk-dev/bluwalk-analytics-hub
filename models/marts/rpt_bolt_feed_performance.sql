SELECT 
  perf.user_location,
  intervals.start_date,
  COUNT(DISTINCT perf.contact_id) as nr_drivers,
  SUM(perf.net_earnings) as net_earnings,
  SUM(perf.nr_trips) as trips,
  SUM(perf.online_minutes) / 60 as online_hours
FROM {{ ref('fct_user_rideshare_performance') }} perf
JOIN {{ ref('util_calendar') }} cal ON perf.date = cal.date
JOIN {{ ref('util_week_intervals') }} intervals ON cal.year_week = intervals.year_week
WHERE perf.partner_name = 'Uber' AND start_date > '2023-01-01'
GROUP BY perf.user_location, intervals.start_date
ORDER BY perf.user_location DESC, intervals.start_date DESC