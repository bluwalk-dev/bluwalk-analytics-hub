-- A query to calculate the number of churns per month from the user monthly churns fact table
SELECT 
    year_month, -- The month and year grouping for churn data
    partner_marketplace,
    count(*) as nr_churns -- The total number of churns that occurred in each year_month
FROM {{ ref('int_retention_monthly_churns_list') }} lcu -- Reference to the user monthly churns fact table in the DBT project
GROUP BY year_month, partner_marketplace -- Grouping the data by year_month to aggregate churns
ORDER BY year_month DESC -- Ordering the results from the most recent month to the oldest