SELECT 
    a.next_year_month as year_month, -- The first month when a user is missing activity
    a.lost_contact_id as contact_id, -- The contact ID associated with the lost user
    a.lost_user_id as user_id -- The ID of the lost user
FROM (
    -- Subquery to find the distinct 'lost' contacts and users by comparing consecutive months
    SELECT DISTINCT
        au.year_month, -- The month of user activity
        au.contact_id as lost_contact_id, -- Contact ID to track if it's missing in the next month
        au.user_id as lost_user_id, -- User ID to track if it's missing in the next month
        ymk.next_year_month -- The subsequent month to check for user activity
    FROM {{ ref('agg_wm_daily_activity') }} au -- Sources user activity data
    LEFT JOIN {{ ref('util_month_intervals') }} ymk ON au.year_month = ymk.year_month -- Joins to identify the next month
    WHERE 
        ymk.end_date <= current_date -- Ensures the data is up to the current date
) a
LEFT JOIN (
    SELECT * FROM {{ ref('agg_wm_daily_activity') }} 
    ) b ON 
        b.year_month = a.next_year_month AND
        a.lost_user_id = b.user_id
WHERE 
    b.user_id IS NULL AND -- Filters to include only users who are not active in the subsequent month
    a.lost_user_id IS NOT NULL  -- Excludes records without a valid user ID
ORDER BY a.next_year_month DESC