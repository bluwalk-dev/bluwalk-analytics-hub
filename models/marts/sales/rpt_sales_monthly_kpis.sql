SELECT 
    year_month,
    bonus,
    insurance_deal_value
FROM {{ ref('agg_sales_monthly_bonus') }}
WHERE 
    year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
ORDER BY year_month DESC