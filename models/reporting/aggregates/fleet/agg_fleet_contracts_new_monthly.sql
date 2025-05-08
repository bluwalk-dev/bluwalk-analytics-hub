SELECT 
    c.year_month, 
    count(*) as new_customers
FROM {{ ref('stg_hubspot_drivfit__deals') }} d
LEFT JOIN {{ ref('util_calendar') }} c ON CAST(d.close_date AS DATE) = c.date
WHERE
    deal_pipeline_id = '586124258' AND
    is_closed_won = true
GROUP BY year_month