WITH 

fuel AS (
  SELECT
      f.statement,
      c.user_name as driver_name,
      c.user_vat as driver_vat,
      c.user_city as city,
      f.partner_name as supplier_name,
      f.card_name,
      f.quantity,
      CASE
        WHEN b.product_id = 33 THEN 'diesel'
        WHEN b.product_id = 35 THEN 'gasoline'
        ELSE NULL
      END energy_source,
      b.amount as ba_debit
    FROM {{ ref('base_service_orders_fuel') }} f
    LEFT JOIN (
        SELECT order_id as id, product_id, -1 * sum(amount) as amount 
        FROM {{ ref('fct_financial_user_transactions') }} 
        WHERE 
            order_type = 'Fuel' AND 
            product_id IN (33, 35) -- only diesel and gasoline
        GROUP BY product_id, order_id
    ) b on b.id = f.energy_id
    LEFT JOIN {{ ref('dim_users') }} c on f.user_id = c.user_id
), 

invoices AS (
  SELECT 
    b.year_week, 
    a.energy_source,
    a.card_name, 
    SUM(a.value_total) as invoiced_value 
  FROM {{ ref('fct_financial_energy_invoiced_transactions') }} a
  LEFT JOIN {{ ref('util_calendar') }} b on DATE_SUB(a.invoice_date, INTERVAL 3 DAY) = b.date
  GROUP BY b.year_week, a.card_name, a.energy_source
)

SELECT
  base.*, 
  i.invoiced_value
FROM (
  SELECT
    ff.statement,
    ff.driver_name,
    ff.driver_vat,
    if (city = 'Madeira', 'Madeira', 'Continente') as city,
    supplier_name,
    card_name,
    ff.energy_source,
    sum(quantity) as quantity,
    sum(ff.ba_debit) as value
  FROM fuel ff
  GROUP BY driver_name, driver_vat, supplier_name, card_name, city, ff.statement, ff.energy_source
  ) base
LEFT JOIN invoices i ON base.card_name = i.card_name AND base.statement = i.year_week AND i.energy_source = base.energy_source
ORDER BY statement DESC