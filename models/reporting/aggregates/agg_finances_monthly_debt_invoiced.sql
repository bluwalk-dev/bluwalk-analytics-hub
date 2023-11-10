SELECT
    year_month,
    ROUND(sum(amount_total),2) amount_invoiced
FROM {{ ref('fct_user_financial_debt_invoices') }}
GROUP BY year_month
ORDER BY year_month DESC