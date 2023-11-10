SELECT
  a.year_week,
  a.start_date,
  a.end_date,
  e.payment_delay,
  f.release_delay
FROM {{ ref('util_week_intervals') }} a
LEFT JOIN {{ ref('agg_finances_weekly_user_invoice_payment_delay') }} e ON a.year_week = e.year_week
LEFT JOIN {{ ref('agg_finances_weekly_statement_close') }} f ON a.year_week = f.year_week
WHERE 
    a.start_date <= current_date AND 
    a.start_date >= '2021-01-01'
ORDER BY year_week DESC