WITH 
    -- CTE 'open_deals' to select open deals for the 'Work' marketplace
    open_deals AS (
        SELECT
            user_id,           -- User ID from the deals fact table
            partner_name       -- Partner name from the deals fact table
        FROM {{ ref("fct_deals") }}
        WHERE
            is_closed = FALSE AND -- Filter for open deals
            partner_marketplace = 'Work' -- Filter for 'Work' marketplace
    ),
    -- CTE 'activity_data' to gather user's first work date across various categories
    activity_data AS (
            -- Ridesharing activity data
            SELECT
                user_id,
                partner_name,
                MIN(CAST(request_timestamp AS DATE)) first_work_date -- Earliest ridesharing work date
            FROM {{ ref("fct_user_rideshare_trips") }}
            WHERE CAST(request_local_time AS DATE) > CAST(DATE_SUB(current_date(), INTERVAL 15 DAY) AS DATE)
            GROUP BY user_id, partner_name

            UNION ALL

            -- Groceries activity data
            SELECT
                user_id,
                'Mercadão' partner_name, -- Hardcoded partner name for groceries
                MIN(date) first_work_date -- Earliest grocery order date
            FROM {{ ref("base_mercadao_orders") }}
            WHERE date > CAST(DATE_SUB(current_date, INTERVAL 15 DAY) AS DATE)
            GROUP BY user_id, partner_name

            UNION ALL

            -- Parcel activity data
            SELECT
                user_id,
                'Correos Express' partner_name, -- Hardcoded partner name for parcel
                MIN(date) first_work_date -- Earliest parcel order date
            FROM {{ ref("base_correos_express_orders") }}
            WHERE date > CAST(DATE_SUB(current_date, INTERVAL 15 DAY) AS DATE)
            GROUP BY user_id, partner_name

            UNION ALL

            -- Food Delivery activity data
            SELECT
                user_id,
                partner_name,
                MIN(end_date) first_work_date -- Earliest food delivery work date
            FROM {{ ref("fct_work_orders") }}
            WHERE
                partner_category = 'Food Delivery' AND
                end_date > CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY) AS DATE)
            GROUP BY user_id, partner_name
    )

-- Final query to join open deals with the activity data
SELECT
  b.user_id,              -- User ID from the activity data
  b.partner_name,         -- Partner name from the activity data
  b.first_work_date       -- First work date from the activity data
FROM open_deals a
LEFT JOIN activity_data b
ON a.partner_name = b.partner_name AND a.user_id = b.user_id
WHERE b.user_id IS NOT NULL -- Filter to include only matched records
