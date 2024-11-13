-- Start with a reference to the 'util_calendar' table which likely contains all dates to ensure a continuous date range.
SELECT 
    a.date,
    -- COALESCE ensures that a NULL value is treated as 0 for the count of missed calls.
    COALESCE(c.nr_missed_calls,0) as nr_missed_calls,
    -- COALESCE ensures that a NULL value is treated as 0 for the count of inbound calls.
    COALESCE(b.nr_inbound_calls,0) as nr_inbound_calls,
    -- Calculate the ratio of missed calls to inbound calls, rounded to 4 decimal places.
    -- Only calculate this ratio if there are inbound calls to avoid division by zero.
    CASE
        WHEN COALESCE(b.nr_inbound_calls, 0) > 0 THEN
            ROUND(c.nr_missed_calls / NULLIF(b.nr_inbound_calls, 0),4)
        ELSE
            NULL
    END AS missed_call_ratio
FROM {{ ref('util_calendar') }} a -- Reference to a calendar model for a continuous series of dates.
LEFT JOIN (
    -- Subquery to count inbound calls per day for specific internal lines.
    SELECT
        CAST(created_at AS DATE) as date, -- Cast end_time to DATE to ignore time part.
        count(*) as nr_inbound_calls
    FROM {{ ref('base_calls_inbound_team') }} -- Reference to a model that contains call data.
    WHERE 
        team = 'Activation'
    GROUP BY date -- Group the count by date.
) b ON a.date = b.date -- Join on date to match inbound calls with calendar dates.
LEFT JOIN (
    -- Subquery to count missed inbound calls per day for specific internal lines.
    SELECT
        CAST(created_at AS DATE) as date,
        count(*) as nr_missed_calls
    FROM {{ ref('base_calls_inbound_team') }}
    WHERE 
        team = 'Activation' AND
        answered_at IS NULL
    GROUP BY date
) c ON a.date = c.date -- Join on date to match missed calls with calendar dates.
-- Filter for dates up to the current date to exclude future dates.
WHERE a.date <= current_date 
ORDER BY a.date DESC -- Order the results starting from the most recent date.
