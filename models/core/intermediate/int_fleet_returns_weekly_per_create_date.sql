select 
  b.year_week,
  count(*) new_returns
from bluwalk-analytics-hub.staging.stg_odoo_flt_booking_returns a
left join bluwalk-analytics-hub.core.ref_calendar b on cast(a.create_date as date) = b.date
where booking_return_type = 'definitive' and booking_return_state IN ('confirm','close')
group by year_week
order by year_week desc