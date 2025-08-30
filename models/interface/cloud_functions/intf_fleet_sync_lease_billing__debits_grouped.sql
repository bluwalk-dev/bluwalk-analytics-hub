select 
  a.opl_conditions_id as conditions_id,
  a.product_id,
  payment_cycle,
  SUM(amount) AS sum_debit
from {{ ref('fct_fleet_billable_items') }} a
left join {{ ref('fct_fleet_rental_contracts') }} b on a.contract_id = b.vehicle_contract_id
where 
  vehicle_contract_length = 'mid-term' and 
  status = 'invoiced' and
  opl_conditions_id is not null
group by conditions_id, product_id, payment_cycle