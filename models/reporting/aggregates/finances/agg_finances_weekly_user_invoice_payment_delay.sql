SELECT 
    year_week,
    ROUND(AVG(CASE
        WHEN DATETIME_DIFF(payment_date, create_date, HOUR) < 0 THEN NULL
        ELSE DATETIME_DIFF(payment_date, create_date, HOUR) 
    END), 2) payment_delay
FROM {{ ref('fct_financial_user_invoices') }}
GROUP BY year_week
ORDER BY year_week DESC