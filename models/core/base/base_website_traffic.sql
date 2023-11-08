-- Define a Common Table Expression (CTE) named 'UserInfoGA3'
WITH
  UserInfoGA3 AS (
    -- Select the date, user identifier, and a flag indicating if the user is new
    SELECT
      PARSE_DATE ("%Y%m%d", event_date) as date, -- Parse the event_date string into a DATE type
      user_pseudo_id, -- A unique identifier for the user
      -- Use a CASE expression to check if the event_name indicates a new user visit, and flag as TRUE if so
      MAX(IF(event_name IN ('first_visit', 'first_open'), TRUE, FALSE)) AS is_new_user
    FROM {{ ref('stg_google_analytics__ga3_events') }} -- Reference the GA3 events staging table
    -- Group by date and user id to aggregate events per user per day
    GROUP BY 1, 2
  ),
  -- Define another CTE named 'UserInfoGA4' in a similar way to 'UserInfoGA3'
  UserInfoGA4 AS (
    SELECT
      PARSE_DATE ("%Y%m%d", event_date) as date, -- Parse the event_date string into a DATE type
      user_pseudo_id, -- A unique identifier for the user
      -- Use a CASE expression to check if the event_name indicates a new user visit, and flag as TRUE if so
      MAX(IF(event_name IN ('first_visit', 'first_open'), TRUE, FALSE)) AS is_new_user    
    FROM {{ ref('stg_google_analytics__ga4_events') }} -- Reference the GA4 events staging table
    -- Apply a filter to consider only events after a certain date
    WHERE event_date > '2022-09-11'
    -- Group by date and user id to aggregate events per user per day
    GROUP BY 1, 2
  )

-- The main SELECT statement combines the results from the two CTEs
SELECT
  * -- Select all columns from the combined dataset
FROM 
  (SELECT * FROM UserInfoGA3 -- Select everything from the GA3 user info CTE
  UNION ALL -- Combine with all rows from the GA4 user info CTE, including duplicates
  SELECT * FROM UserInfoGA4)
ORDER BY date DESC -- Order the combined dataset by date in descending order
