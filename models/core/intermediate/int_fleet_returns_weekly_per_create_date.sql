select 
  b.year_week,
  count(*) new_returns
from {{ ref('stg_odoo_drivfit__booking_returns') }} a
left join {{ ref('util_calendar') }} b on cast(a.create_date as date) = b.date
where return_type = 'definitive' and state IN ('confirm','close')
group by year_week
order by year_week desc