SELECT 
    year_month,
    nr_won_deals 
FROM {{ ref('agg_sales_monthly_pipeline_deal_won') }} 
WHERE deal_pipeline_id = 'default'