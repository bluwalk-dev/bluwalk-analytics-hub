SELECT 
    year_month,
    count(*) as nr_churns
FROM {{ ref('int_user_monthly_churns_list') }} lcu
GROUP BY year_month
ORDER BY year_month DESC