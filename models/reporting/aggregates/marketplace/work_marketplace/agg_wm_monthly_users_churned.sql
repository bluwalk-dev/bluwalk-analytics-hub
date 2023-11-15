-- A query to calculate the number of churns per month from the user monthly churns fact table
SELECT 
    year_month, -- The month and year grouping for churn data
    count(*) as nr_churns -- The total number of churns that occurred in each year_month
FROM {{ ref('agg_activation_monthly_churns') }} lcu -- Reference to the user monthly churns fact table in the DBT project
GROUP BY year_month -- Grouping the data by year_month to aggregate churns
ORDER BY year_month DESC -- Ordering the results from the most recent month to the oldest