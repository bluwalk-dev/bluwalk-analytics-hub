select
    b.year_month,
    sum(a.amount) as insurance_revenue
from {{ ref('fct_insurance_policy_payments') }} a
join {{ ref('util_calendar') }} b
    on a.start_date = b.date
group by b.year_month