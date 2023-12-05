-- CTE: vehicle_usage
-- This section creates a temporary table 'vehicle_usage' to hold distinct records of vehicle usage.
WITH vehicle_usage AS (
    -- Selecting distinct values to avoid duplicates.
    SELECT DISTINCT
        -- Casting the dropoff timestamp to a date format to consider only the date part.
        CAST(a.dropoff_timestamp AS DATE) date,
        a.user_id,
        a.vehicle_contract_id,
        -- Including start date and service fee from the vehicle contracts.
        b.start_date,
        b.service_fee
    FROM 
        -- Referring to the 'fct_user_rideshare_trips' table using DBT's ref function.
        {{ ref('fct_user_rideshare_trips') }} a
    LEFT JOIN 
        -- Joining with 'dim_vehicle_contracts' table to get contract-related info.
        {{ ref('dim_vehicle_contracts') }} b ON a.vehicle_contract_id = b.vehicle_contract_id
    WHERE 
        -- Filtering out records where user_id or vehicle_contract_id is null.
        a.user_id is not null AND 
        a.vehicle_contract_id is not null
)

-- Main Query
-- This section is the main query that fetches the final result set.
SELECT 
    -- Selecting date, user_id, and service_fee from the 'vehicle_usage' CTE.
    b.date, 
    b.user_id, 
    b.service_fee 
FROM (
    -- Subquery to get the latest start date for each user and date combination.
    SELECT 
        date, 
        user_id, 
        -- Using max to get the most recent start_date for each user on each date.
        max(start_date) start_date
    FROM 
        vehicle_usage
    -- Grouping by date and user_id to prepare for the latest start_date selection.
    GROUP BY 
        date, user_id
) a
-- Joining the subquery result back to the 'vehicle_usage' CTE.
LEFT JOIN 
    vehicle_usage b ON 
    -- Matching records based on the same date, user_id, and the most recent start_date.
    a.date = b.date AND 
    a.user_id = b.user_id AND 
    a.start_date = b.start_date
-- Ordering the results by date and user_id in descending order.
ORDER BY 
    date DESC, 
    user_id DESC