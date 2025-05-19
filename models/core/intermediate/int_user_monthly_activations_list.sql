-- The query starts with a subquery to find the first activation per user
SELECT * FROM (
    
    /* First Activation */
    -- This subquery is selecting the earliest (first) activation for each user
    SELECT
        user_id, -- The ID of the user
        contact_id, -- The associated contact ID for the user
        min(year_month) as year_month, -- The earliest month of user activity indicating first activation
        'new' as activation_type -- Labeling this row as representing a 'new' activation
    FROM {{ ref('agg_wm_daily_activity') }} -- Reference to a user activity fact table
    GROUP BY user_id, contact_id
    -- Note: The original GROUP BY included 'partner_id', 'partner_name', and 'partner_stream' which are not selected,
    -- this seems to be an error and might cause the query to fail. They should either be included in the SELECT or removed from GROUP BY.

    UNION ALL

    /* Subsequent Activations */
    -- This subquery finds activations that occur after the user has churned at least once
    SELECT 
        au.user_id, -- The ID of the user
        au.contact_id, -- The associated contact ID for the user
        min(au.year_month) AS year_month, -- The earliest month of reactivation after being churned
        'reactivation' as activation_type -- Labeling this row as representing a 'reactivation'
    FROM 
        -- A derived table of all user activities
        (SELECT 
            user_id,
            contact_id,
            year_month
        FROM {{ ref('agg_wm_daily_activity') }}) au
        
        -- This CROSS JOIN will combine all records from au with all records from lcu, 
        -- which is typically not performant and may not be necessary.
        -- It should likely be an INNER JOIN with a condition to match user_id for both sides.
        CROSS JOIN
        
        -- A derived table of all monthly churns
        (SELECT 
            year_month, 
            user_id 
        FROM {{ ref('int_user_monthly_churns_list') }}) lcu
    WHERE 
        au.year_month > lcu.year_month AND -- The condition that ensures we're looking at activity after a churn
        au.user_id = lcu.user_id
    
    GROUP BY lcu.year_month, au.user_id, au.contact_id
    -- Note: Since 'lcu.year_month' is used in the GROUP BY, it should be included in the SELECT clause, or it could lead to an error.
)
ORDER BY year_month DESC