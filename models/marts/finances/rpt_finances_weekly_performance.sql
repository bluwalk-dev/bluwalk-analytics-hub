-- Selects weekly data from various financial tables to show invoiced debt and collection ratios including any delays in payments or statement releases
SELECT
  a.year_week, -- Selects the year and week from the utility week intervals table
  a.start_date, -- Selects the start date of the week interval
  a.end_date, -- Selects the end date of the week interval
  e.payment_delay, -- Selects the average delay in invoice payments for that week
  f.release_delay, -- Selects the average delay in statement releases for that week
  g.amount_invoiced AS invoiced_debt, -- Selects the amount invoiced for that week, aliasing it as 'invoiced_debt'
  g.fast_collection_ratio -- Selects the ratio of fast debt collection for that week
FROM 
    {{ ref('util_week_intervals') }} a -- References the table that holds week interval data
LEFT JOIN 
    {{ ref('agg_finances_weekly_user_invoice_payment_delay') }} e ON a.year_week = e.year_week -- Joins with the table that aggregates payment delay data on year_week
LEFT JOIN 
    {{ ref('agg_finances_weekly_statement_close') }} f ON a.year_week = f.year_week -- Joins with the table that aggregates statement release delay data on year_week
LEFT JOIN 
    {{ ref('agg_finances_weekly_fast_debt_collection') }} g ON a.year_week = g.year_week -- Joins with the table that aggregates fast debt collection data on year_week
WHERE 
    a.start_date <= current_date AND -- Filters records to those starting on or before the current date
    a.start_date >= '2021-01-01' -- Filters records to those starting on or after January 1st, 2021
ORDER BY 
    year_week DESC -- Orders the results by year and week in descending order to get the most recent data first