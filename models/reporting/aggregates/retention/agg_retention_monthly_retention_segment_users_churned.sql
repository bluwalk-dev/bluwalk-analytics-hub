-- A query to calculate the number of churns per month from the user monthly churns fact table
SELECT 
    year_month, -- The month and year grouping for churn data
    retention_segment,
    count(*) as nr_churns -- The total number of churns that occurred in each year_month
FROM {{ ref('int_retention_monthly_retention_segment_churns_list') }} lcu -- Reference to the user monthly churns fact table in the DBT project
GROUP BY
    year_month,
    retention_segment
ORDER BY 
    year_month DESC, -- Ordering the results from the most recent month to the oldest
    retention_segment DESC