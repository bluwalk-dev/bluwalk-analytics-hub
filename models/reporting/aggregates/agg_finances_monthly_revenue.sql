SELECT
    b.year_month,
    ROUND(SUM(a.amount), 2) amount
FROM {{ ref('fct_accounting_revenue') }} a
LEFT JOIN {{ ref('util_calendar') }} b ON a.date = b.date
WHERE
    a.date <= current_date
GROUP BY b.year_month
ORDER BY b.year_month DESC