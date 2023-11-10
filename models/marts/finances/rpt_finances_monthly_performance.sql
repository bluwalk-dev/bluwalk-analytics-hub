SELECT
  a.year_month,
  COALESCE(b.amount_invoiced, 0) amount_invoiced,
  COALESCE(c.amount_recovered, 0) amount_recovered,
  COALESCE(d.amount_recovered_non_cash, 0) amount_recovered_non_cash,
  COALESCE(d.collection_success_ratio, 0) collection_success_ratio,
  e.payment_delay,
  f.release_delay,
  g.estimation_lt,
  g.definitive_lt
FROM {{ ref('util_month_intervals') }} a
LEFT JOIN {{ ref('agg_finances_monthly_debt_invoiced') }} b ON a.year_month = b.year_month
LEFT JOIN {{ ref('agg_finances_monthly_debt_collection') }} c ON a.year_month = c.year_month
LEFT JOIN {{ ref('agg_finances_monthly_collection_success') }} d ON a.year_month = d.year_month
LEFT JOIN {{ ref('agg_finances_monthly_user_invoice_payment_delay') }} e ON a.year_month = e.year_month
LEFT JOIN {{ ref('agg_finances_monthly_statement_close') }} f ON a.year_month = f.year_month
LEFT JOIN {{ ref('stg_google_sheets__accounting_close') }} g ON a.year_month = g.year_month
WHERE 
    a.start_date <= current_date AND 
    a.start_date >= '2021-01-01'
ORDER BY year_month DESC