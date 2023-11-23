-- This SELECT statement is meant to calculate churn rates for each month.
SELECT 
    a.year_month, -- Select the year and month from the churned users table.
    a.partner_key,
    a.partner_marketplace,
    a.partner_category,
    a.partner_name,
    b.nr_active_users as start_active_users, -- The number of active users at the start of the period.
    a.nr_churns as nr_churns,  -- The number of users who churned during the month.
    
    -- This CASE statement calculates the churn rate.
    CASE 
        -- If there are no active users, then churn rate is not applicable, hence NULL.
        WHEN b.nr_active_users = 0 THEN null 
        -- Otherwise, the churn rate is the number of churns divided by the number of active users.
        ELSE ROUND(a.nr_churns / b.nr_active_users, 4)
    END as churn_rate
    
-- The FROM clause indicates that the data is coming from the monthly churned users table.
FROM {{ ref('agg_cm_monthly_partner_users_churned') }} a

-- A LEFT JOIN is used to bring in the utility month intervals which likely contains metadata about the months.
LEFT JOIN {{ ref('util_month_intervals') }} c ON a.year_month = c.year_month

-- Another LEFT JOIN is used to bring in the number of active users from the previous month,
-- since churn rate is typically calculated as churns in the current month over active users from the previous month.
LEFT JOIN {{ ref('agg_cm_monthly_partner_users_active') }} b ON 
    a.partner_key = b.partner_key AND
    a.partner_name = b.partner_name AND
    a.partner_marketplace = b.partner_marketplace AND
    c.previous_year_month = b.year_month
    
-- The results are ordered by year_month in descending order to show the most recent data first.
ORDER BY 
    a.year_month DESC,
    a.partner_marketplace DESC