SELECT 
    a.year_month,
    b.bonus
FROM {{ ref('util_month_intervals') }} a
LEFT JOIN {{ ref('agg_sales_monthly_bonus') }} b ON a.year_month = b.year_month
WHERE 
    a.year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
ORDER BY a.year_month DESC