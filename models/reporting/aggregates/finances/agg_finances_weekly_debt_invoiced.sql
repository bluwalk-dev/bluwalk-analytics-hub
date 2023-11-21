SELECT
    year_week,
    ROUND(sum(amount_total),2) amount_invoiced
FROM {{ ref('fct_financial_user_debt_invoices') }}
GROUP BY year_week
ORDER BY year_week DESC