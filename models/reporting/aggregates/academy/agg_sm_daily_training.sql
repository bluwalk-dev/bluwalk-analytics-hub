WITH training_payments AS (
  SELECT
    DATE(so.date_order) AS date,
    COUNTIF(STRPOS(sol.name, 'ONLINE') > 0) AS online_trainings,
    COUNTIF(STRPOS(sol.name, 'ONLINE') = 0) AS onsite_trainings,
    COUNT(*) AS total_trainings
  FROM {{ ref('stg_odoo__sale_order_lines') }} sol
  JOIN {{ ref('stg_odoo__sale_orders') }} so
    ON sol.order_id = so.id
  WHERE sol.event_id IS NOT NULL
    AND sol.price_total = 50
    AND so.state = 'sale'
  GROUP BY date
)

SELECT
  cal.date,
  cal.year_week,
  cal.year_month,
  COALESCE(tp.online_trainings,  0) AS online_trainings,
  COALESCE(tp.onsite_trainings,  0) AS onsite_trainings,
  COALESCE(tp.total_trainings,   0) AS total_trainings
FROM {{ ref('util_calendar') }} cal
LEFT JOIN training_payments tp
  ON cal.date = tp.date
WHERE 
    cal.date BETWEEN (SELECT MIN(date) FROM training_payments)
    AND (SELECT MAX(date) FROM training_payments)
ORDER BY cal.date