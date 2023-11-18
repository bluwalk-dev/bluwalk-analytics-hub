SELECT
  date,
  REPLACE(REPLACE(analytic_account_name,'ACC/GP/',''),'ACC/REV/','') as revenue_stream,
  ROUND(amount,2) as amount
FROM {{ ref('fct_accounting_analytic_lines') }}
WHERE 
    analytic_account_name like 'ACC/GP/%' OR 
    analytic_account_name = 'ACC/REV/Global Invoicing' OR 
    analytic_account_name = 'ACC/REV/Events'
ORDER BY date DESC