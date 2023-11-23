-- Top-level SELECT statement to calculate the number of active users per year_month
SELECT 
    year_month, -- The month and year for which the data is aggregated
    partner_key,
    partner_marketplace,
    partner_category,
    partner_name,
    count(*) as nr_active_users -- Count of distinct user and contact ID combinations per year_month
FROM (
    -- Subquery to select distinct user activity by year_month
    SELECT DISTINCT
        year_month, -- The month and year of the activity
        user_id, -- The unique identifier of a user
        contact_id, -- The unique identifier of a contact (possibly associated with the user)
        partner_key,
        partner_marketplace,
        partner_category,
        partner_name
    FROM {{ ref('agg_cm_daily_activity') }} -- Reference to the user activity fact table within the DBT model
    -- Using DISTINCT ensures that each user_id and contact_id combination is counted only once per year_month
)
-- GROUP BY to aggregate the distinct user activity counts by year_month
GROUP BY 
    year_month, -- The month and year for which the data is aggregated
    partner_marketplace,
    partner_category,
    partner_name,
    partner_key
-- ORDER BY to ensure the results are listed with the most recent month first
ORDER BY year_month DESC