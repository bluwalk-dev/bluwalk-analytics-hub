select 
  a.period_start AS start_date,
  a.period_end AS end_date,
  a.description AS description,
  b.vehicle_contract_id as lease_contract_id,
  a.opl_conditions_id as conditions_id,
  a.product_id,
  amount AS debit,
  quantity,
  payment_cycle
from {{ ref('fct_fleet_billable_items') }} a
left join bluwalk-analytics-hub.core.core_fleet_rental_contracts b on a.contract_id = b.vehicle_contract_id
where 
  vehicle_contract_length = 'mid-term' and 
  status = 'invoiced' and
  opl_conditions_id is not null and
  product_id = 356