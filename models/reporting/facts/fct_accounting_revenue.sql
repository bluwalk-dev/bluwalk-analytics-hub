SELECT
  date,
  REPLACE(REPLACE(account_name,'ACC/GP/',''),'ACC/REV/','') as revenue_stream,
  ROUND(amount,2) as amount
FROM {{ ref('fct_accounting_analytic_lines') }}
WHERE 
    account_name like 'ACC/GP/%' OR 
    account_name = 'ACC/REV/Global Invoicing' OR 
    account_name = 'ACC/REV/Events'
ORDER BY date DESC