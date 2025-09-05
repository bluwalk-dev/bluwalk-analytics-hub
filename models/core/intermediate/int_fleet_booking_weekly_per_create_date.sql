select 
  b.year_week,
  count(*) new_bookings
from {{ ref('stg_odoo_drivfit__bookings') }} a
left join {{ ref('util_calendar') }} b on cast(a.create_date as date) = b.date
where booking_type = 'new' and state IN ('open', 'close', 'confirm')
group by year_week
order by year_week desc