select 
  b.year_week,
  count(*) new_bookings
from bluwalk-analytics-hub.core.core_fleet_rental_bookings a
left join bluwalk-analytics-hub.core.ref_calendar b on cast(a.create_date as date) = b.date
where booking_type = 'new' and booking_state IN ('open', 'close', 'confirm')
group by year_week
order by year_week desc