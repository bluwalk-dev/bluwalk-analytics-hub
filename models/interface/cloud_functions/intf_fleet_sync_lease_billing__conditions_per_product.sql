SELECT
  a.lease_contract_id,
  b.vehicle_contract_name as lease_contract_name,
  a.conditions_id,
  a.debit_type,
  a.effective_date,
  a.termination_date,
  a.product_id,
  b.customer_id,
  b.vehicle_id,
  a.wk_price,
  CAST(b.end_date_estimated AS DATE) end_date_estimated
FROM (
  SELECT
    id as conditions_id,
    lease_contract_id,
    'base_rent' as debit_type,
    323 as product_id,
    effective_date,
    termination_date,
    rate_base_wk_price as wk_price
  FROM {{ ref('stg_odoo_drivfit__lease_contract_conditions') }}

  UNION ALL

  SELECT
    id as conditions_id,
    lease_contract_id,
    'mileage_pack',
    353 as product_id,
    effective_date,
    termination_date,
    rate_mileage_wk_price as wk_price
  FROM {{ ref('stg_odoo_drivfit__lease_contract_conditions') }}
  WHERE rate_mileage_wk_price > 0

  UNION ALL

  SELECT
    id as conditions_id,
    lease_contract_id,
    'protection_pack',
    354 as product_id,
    effective_date,
    termination_date,
    rate_protection_wk_price as wk_price
  FROM {{ ref('stg_odoo_drivfit__lease_contract_conditions') }}
  WHERE rate_protection_wk_price > 0
) a
LEFT JOIN {{ ref('fct_fleet_rental_contracts') }} b ON a.lease_contract_id = b.vehicle_contract_id
WHERE vehicle_contract_length = 'mid-term'
ORDER BY a.effective_date DESC