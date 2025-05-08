SELECT
  c.date,
  t.contract_id,
  t.contract_type,
  t.contract_name,
  ROUND(SUM(
    (CASE WHEN t.amount > 0 THEN 1 ELSE -1 END) *
    (CASE WHEN t.contract_type = 'short-term' THEN t.quantity/24 ELSE 1 END)
  ), 2) AS total_days
FROM {{ ref('fct_fleet_billable_items') }} t
JOIN {{ ref('util_calendar') }} c
  ON c.date BETWEEN t.period_start AND t.period_end
WHERE t.product_id = 323
GROUP BY
  c.date,
  t.contract_id,
  t.contract_type,
  t.contract_name
HAVING ROUND(SUM(
    (CASE WHEN t.amount > 0 THEN 1 ELSE -1 END) *
    (CASE WHEN t.contract_type = 'short-term' THEN t.quantity/24 ELSE 1 END)
  ), 2) > 0
ORDER BY c.date