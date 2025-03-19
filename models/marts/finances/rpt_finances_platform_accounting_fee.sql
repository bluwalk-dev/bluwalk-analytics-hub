select
    b.year_month,
    CASE
        WHEN entity_id = 1771 THEN 'Uber'
        WHEN entity_id = 2189 THEN 'Bolt'
        ELSE NULL
    END partner,
    SUM(a.balance) as total_cost
from {{ ref('fct_accounting_move_lines') }} a
left join {{ ref('util_calendar') }} b on a.date = b.date
where 
  financial_system_id = 4 and 
  account_id = 3086
group by b.year_month, partner
order by b.year_month desc