WITH reporting_structure AS (
    SELECT * 
    FROM {{ ref('dim_accounting_reporting_structure') }}
)

/*
SELECT
    d.year_month,
    a.date,
    IFNULL(c.reporting_business, f.reporting_business) reporting_business,
    IFNULL(c.reporting_level_1, f.reporting_level_1) reporting_level_1, 
    IFNULL(c.reporting_level_2, f.reporting_level_2) reporting_level_2,
    IFNULL(c.reporting_level_3, f.reporting_level_3) reporting_level_3,
    IFNULL(c.reporting_level_4, f.reporting_level_4) reporting_level_4,
    a.name,
    a.move_name,
    a.account_name,
    a.account_code,
    a.analytic_account_name,
    a.balance
FROM {{ ref('fct_accounting_move_lines') }} a
LEFT JOIN {{ ref('dim_accounting_accounts') }} b ON a.account_code = b.account_code
LEFT JOIN reporting_structure c ON a.account_code = c.account_code
LEFT JOIN reporting_structure f ON a.analytic_account_name = f.analytic_account_name
LEFT JOIN {{ ref('util_calendar') }} d ON a.date = d.date
WHERE
    (a.account_code like '6%' or a.account_code like '7%') 
    AND move_state = 'posted'

UNION ALL
*/

SELECT
    d.year_month,
    a.date,
    f.reporting_business,
    f.reporting_level_1, 
    f.reporting_level_2,
    f.reporting_level_3,
    f.reporting_level_4,
    a.name,
    NULL move_name,
    NULL account_name,
    NULL account_code,
    a.analytic_account_name,
    -1 * a.amount balance
FROM {{ ref('fct_accounting_analytic_lines') }} a
LEFT JOIN reporting_structure f ON a.analytic_account_name = f.analytic_account_name
LEFT JOIN {{ ref('util_calendar') }} d ON a.date = d.date
WHERE
    a.analytic_account_name like 'ACC/%'