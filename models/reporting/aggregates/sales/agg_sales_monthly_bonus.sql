SELECT 
    year_month, 
    SUM(bonus) bonus
FROM {{ ref('agg_sales_monthly_pipeline_bonus') }}
GROUP BY year_month
ORDER BY year_month DESC