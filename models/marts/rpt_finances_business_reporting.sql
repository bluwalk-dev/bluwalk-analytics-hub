WITH analytic_lines AS (
    SELECT 
        b.year_month,
        IF(a.analytic_account_name LIKE 'BA%', 'ACC/COS/User Payouts', a.analytic_account_name) reporting_account_name,
        a.*
    FROM {{ ref('fct_accounting_analytic_lines') }} a
    LEFT JOIN {{ ref('util_calendar') }} b on a.date = b.date
), 

accounting_moves AS (
    SELECT 
        year_month,
        reporting_account_name,
        sum(amount) amount
    from analytic_lines
    where move_id is not null
    group by year_month, reporting_account_name
), 

analytic_moves AS (
    SELECT 
        year_month,
        reporting_account_name,
        sum(amount) amount
    from analytic_lines
    where move_id is null
    group by year_month, reporting_account_name
)

SELECT 
    a.*, 
    c.amount analytic_amount, 
    b.amount accounting_amount
    FROM (
        SELECT DISTINCT year_month, reporting_account_name 
        FROM analytic_lines
    ) a
    LEFT JOIN accounting_moves b on b.year_month = a.year_month and b.reporting_account_name = a.reporting_account_name
    LEFT JOIN analytic_moves c on a.year_month = c.year_month and a.reporting_account_name = c.reporting_account_name
ORDER BY year_month DESC, reporting_account_name ASC