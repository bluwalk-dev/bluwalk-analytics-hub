SELECT
  a.id as transaction_line_id,
  a.name as transaction_line_name,
  b.user_id,
  d.contact_id,
  a.account_id,
  b.transaction_account_name as account_name,
  b.transaction_account_type as account_type,
  a.transaction_name,
  a.transaction_hash,
  a.date,
  a.category_id,
  c.name as category_name,
  a.payment_cycle,
  financial_document_id,
  a.amount,
  a.amount_residual,
  a.amount_balance,
  a.description,
  a.trips_id,
  a.fuel_id,
  a.rental_vehicle_id,
  a.misc_id
FROM {{ ref('stg_odoo__transaction_lines') }} a
LEFT JOIN {{ ref('stg_odoo__transaction_accounts') }} b ON a.account_id = b.transaction_account_id
LEFT JOIN {{ ref('stg_odoo__transaction_categories') }} c ON a.category_id = c.id
LEFT JOIN {{ ref('dim_users') }} d ON b.user_id = d.user_id