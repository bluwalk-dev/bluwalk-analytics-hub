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
      b.amount as ba_debit
    FROM {{ ref('base_service_orders_fuel') }} f
    LEFT JOIN (
        SELECT fuel_id as id, -1 * sum(amount) as amount 
        FROM {{ ref('stg_odoo__account_analytic_lines') }} 
        WHERE group_id is null and fuel_id is not null and product_id = 33 
        GROUP BY fuel_id
    ) b on b.id = f.energy_id
    LEFT JOIN {{ ref('dim_users') }} c on f.user_id = c.user_id
), 

trips AS (
  select
      t.statement,
      rp.user_name display_name,
      rp.user_vat vat,
      sum(t.sales_gross) as tvde_sales,
      total_sales
  from {{ ref('fct_work_orders') }} t
  left join {{ ref('dim_users') }} rp on t.user_id = rp.user_id
  left join (
    select t.statement, rp.user_vat, sum(t.sales_gross) as total_sales
    from {{ ref('fct_work_orders') }} t
    left join {{ ref('dim_users') }} rp on t.user_id = rp.user_id
    group by user_vat, statement
    ) t2 on t2.user_vat = rp.user_vat AND t2.statement = t.statement
  where partner_category = 'TVDE'
  group by rp.user_name, rp.user_vat, t2.total_sales, t.statement
), 

invoices AS (
  SELECT 
    b.year_week, 
    a.card_name, 
    SUM(a.value_total) as invoiced_value 
  FROM {{ ref('fct_financial_energy_invoiced_transactions') }} a
  LEFT JOIN {{ ref('util_calendar') }} b on DATE_SUB(a.invoice_date, INTERVAL 3 DAY) = b.date
  GROUP BY b.year_week, a.card_name
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
    sum(quantity) as quantity,
    sum(ff.ba_debit) as value,
    tf.tvde_sales,
    tf.total_sales
  FROM fuel ff
  LEFT JOIN trips tf on tf.vat = ff.driver_vat AND ff.statement = tf.statement
  GROUP BY driver_name, driver_vat, supplier_name, card_name, tf.tvde_sales, tf.total_sales, city, ff.statement
  ) base
LEFT JOIN invoices i ON base.card_name = i.card_name AND base.statement = i.year_week
ORDER BY statement DESC