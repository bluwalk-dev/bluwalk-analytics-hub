-- This query identifies users who were active in one month but not in the following month, 
-- classifying them as 'lost' users.

SELECT 
    a.next_year_month as year_month, -- The first month when a user is missing activity
    a.partner_marketplace
    a.lost_contact_id as contact_id, -- The contact ID associated with the lost user
    a.lost_user_id as user_id -- The ID of the lost user
FROM (
    -- Subquery to find the distinct 'lost' contacts and users by comparing consecutive months
    SELECT DISTINCT
        au.year_month, -- The month of user activity
        au.partner_marketplace
        au.contact_id as lost_contact_id, -- Contact ID to track if it's missing in the next month
        au.user_id as lost_user_id, -- User ID to track if it's missing in the next month
        ymk.next_year_month -- The subsequent month to check for user activity
    FROM {{ ref('agg_cm_daily_activity') }} au -- Sources user activity data
    LEFT JOIN {{ ref('util_month_intervals') }} ymk ON au.year_month = ymk.year_month -- Joins to identify the next month
    WHERE 
        ymk.end_date <= current_date -- Ensures the data is up to the current date
) a
LEFT JOIN (
    SELECT * FROM {{ ref('agg_cm_daily_activity') }} 
    ) b ON 
        b.year_month = a.next_year_month AND -- Matches to check if the user is active in the subsequent month
        a.lost_user_id = b.user_id -- Ensures the same user is compared across months
        a.partner_marketplace = b.partner_marketplace
WHERE 
    b.user_id IS NULL AND -- Filters to include only users who are not active in the subsequent month
    a.lost_user_id IS NOT NULL  -- Excludes records without a valid user ID
ORDER BY a.next_year_month DESC -- Sorts the result by the month of lost activity, starting with the most recent

-- The output will be a list of user IDs and contact IDs for users who did not have activity
-- in the month following their last recorded activity, implying they have churned or become inactive.