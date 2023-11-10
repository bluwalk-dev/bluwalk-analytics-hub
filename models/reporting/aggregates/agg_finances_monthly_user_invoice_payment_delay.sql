SELECT 
    year_month,
    ROUND(AVG(CASE
        WHEN DATETIME_DIFF(payment_date, create_date, HOUR) < 0 THEN NULL
        ELSE DATETIME_DIFF(payment_date, create_date, HOUR) 
    END), 2) validation_delay
FROM {{ ref('fct_user_financial_invoices') }}
GROUP BY year_month
ORDER BY year_month DESC