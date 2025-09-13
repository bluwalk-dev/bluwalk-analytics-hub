SELECT
  a.period_end AS date,
  a.description AS description,
  NULL AS transaction_type,
  6 AS service_partner_id,
  3066 AS supplier_id,
  d.contact_id AS partner_id,
  round(amount,2) AS cost,
  0 AS commission,
  IF(amount > 0, 
    (CASE
      WHEN product_id = 309 THEN round(amount * 1.23, 2)
      WHEN product_id = 367 THEN round(amount * 1.06, 2)
      ELSE amount
    END)
  , 0) AS debit,
  IF(amount < 0, (CASE
      WHEN product_id = 309 THEN -1 * round(amount * 1.23, 2)
      WHEN product_id = 367 THEN -1 * round(amount * 1.06, 2)
      ELSE -1 * amount
    END), 0) AS credit,
  'import' AS create_method,
  a.payment_cycle AS payment_cycle,
  a.product_id,
  e.analytic_account_id
from {{ ref("fct_fleet_billable_items") }} a
left join bluwalk-analytics-hub.core.core_fleet_rental_contracts_lease b on a.contract_id = b.lease_contract_id
left join bluwalk-analytics-hub.core.core_contacts_flt c on b.driver_id = c.contact_id
left join {{ ref("dim_contacts") }} d on c.contact_vat = d.contact_vat
left join {{ ref("dim_accounting_analytic_accounts") }} e on d.contact_id = e.analytic_account_owner_contact_id
where 
  contract_type IN ('mid-term', 'long-term') and
  status = 'invoiced' and 
  a.customer_id = 21 and
  e.analytic_account_type = 'User'
  
UNION ALL

select 
  a.period_end AS date,
  a.description AS description,
  NULL AS transaction_type,
  6 AS service_partner_id,
  3066 AS supplier_id,
  d.contact_id AS partner_id,
  round(amount,2) AS cost,
  0 AS commission,
  IF(amount > 0, 
    (CASE
      WHEN product_id = 309 THEN round(amount * 1.23, 2)
      WHEN product_id = 367 THEN round(amount * 1.06, 2)
      ELSE amount
    END)
  , 0) AS debit,
  IF(amount < 0, (CASE
      WHEN product_id = 309 THEN -1 * round(amount * 1.23, 2)
      WHEN product_id = 367 THEN -1 * round(amount * 1.06, 2)
      ELSE -1 * amount
    END), 0) AS credit,
  'import' AS create_method,
  a.payment_cycle AS payment_cycle,
  a.product_id,
  e.analytic_account_id
from {{ ref("fct_fleet_billable_items") }} a
left join bluwalk-analytics-hub.core.core_fleet_rental_contracts_rac b on a.contract_id = b.rental_contract_id
left join bluwalk-analytics-hub.core.core_contacts_flt c on b.driver_id = c.contact_id
left join {{ ref("dim_contacts") }} d on c.contact_vat = d.contact_vat
left join {{ ref("dim_accounting_analytic_accounts") }} e on d.contact_id = e.analytic_account_owner_contact_id
where
  contract_type = 'short-term' and
  status = 'invoiced' and 
  a.customer_id = 21 AND
  e.analytic_account_type = 'User'