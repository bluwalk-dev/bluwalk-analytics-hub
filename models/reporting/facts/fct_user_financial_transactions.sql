select
  a.date,
  a.statement,
  c.user_id,
  c.contact_id,
  b.product_group,
  b.product_category,
  b.product_name,
  a.amount,
  a.name description,
  a.order_type,
  a.order_id
FROM {{ ref('fct_accounting_analytic_lines') }} a
LEFT JOIN {{ ref('dim_products') }} b ON a.product_id = b.product_id
LEFT JOIN {{ ref('dim_users') }} c on a.account_owner_contact_id = c.contact_id
WHERE
    b.user_transaction IS TRUE AND 
    a.move_id IS NULL AND
    a.account_type = 'User'
ORDER BY date DESC