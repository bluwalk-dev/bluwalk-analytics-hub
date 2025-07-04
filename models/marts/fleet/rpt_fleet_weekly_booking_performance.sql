SELECT
    a.year_week,
    b.new_bookings,
    c.new_returns,
    (b.new_bookings - c.new_returns) as balance
FROM {{ ref('util_week_intervals') }} a
LEFT JOIN {{ ref('int_fleet_booking_weekly_per_create_date') }} b ON a.year_week = b.year_week
LEFT JOIN {{ ref('int_fleet_returns_weekly_per_create_date') }} c ON a.year_week = c.year_week
WHERE (b.new_bookings is not null or c.new_returns is not null)
ORDER BY year_week DESC