WITH
    legal_agreements AS (
        SELECT DISTINCT contact_id
        FROM {{ ref('dim_legal_agreements') }}
        WHERE 
            legal_agreement_state = 'accepted' AND
            legal_agreement_template_name IN ('Individual Agreement Contract', 'Contrato de Prestação de Serviços - Particulares') AND
            contact_id IS NOT NULL
    ),
    -- CTE 'open_deals' to select open deals for the 'Work' marketplace
    open_deals AS (
        SELECT
            user_id,           -- User ID from the deals fact table
            partner_name,       -- Partner name from the deals fact table
            deal_partner_key AS partner_key
        FROM {{ ref("fct_deals") }}
        WHERE
            is_closed = FALSE AND -- Filter for open deals
            partner_marketplace = 'Work' AND -- Filter for 'Work' marketplace
            deal_pipeline_stage_id NOT IN ('294191047', '307257314', '322811066') -- Except stage Open
    ),
    -- CTE 'activity_data' to gather user's first work date across various categories
    activity_data AS (
            -- Ridesharing activity data
            SELECT
                user_id,
                partner_name,
                partner_key,
                MIN(CAST(request_timestamp AS DATE)) first_work_date -- Earliest ridesharing work date
            FROM {{ ref("fct_user_rideshare_trips") }}
            WHERE CAST(request_local_time AS DATE) > CAST(DATE_SUB(current_date(), INTERVAL 15 DAY) AS DATE)
            GROUP BY user_id, partner_name, partner_key

            UNION ALL

            -- Groceries activity data
            SELECT
                user_id,
                partner_name,
                partner_key,
                MIN(date) first_work_date -- Earliest grocery order date
            FROM {{ ref("base_mercadao_orders") }}
            WHERE date > CAST(DATE_SUB(current_date, INTERVAL 15 DAY) AS DATE)
            GROUP BY user_id, partner_name, partner_key

            UNION ALL

            -- Parcel activity data
            SELECT
                user_id,
                partner_name,
                partner_key,
                MIN(date) first_work_date
            FROM {{ ref("base_correos_express_orders") }} a
            LEFT JOIN legal_agreements b ON a.contact_id = b.contact_id
            WHERE date > CAST(DATE_SUB(current_date, INTERVAL 30 DAY) AS DATE) AND b.contact_id IS NOT NULL
            GROUP BY user_id, partner_name, partner_key

            UNION ALL

            -- Food Delivery activity data
            SELECT
                user_id,
                partner_name,
                partner_key,
                MIN(end_date) first_work_date -- Earliest food delivery work date
            FROM {{ ref("fct_work_orders") }}
            WHERE
                partner_category IN ('Food Delivery', 'Courier') AND
                end_date > CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AS DATE)
            GROUP BY user_id, partner_name, partner_key
    )

-- Final query to join open deals with the activity data
SELECT DISTINCT
  b.user_id,              -- User ID from the activity data
  b.partner_name,         -- Partner name from the activity data
  a.partner_key,
  b.first_work_date       -- First work date from the activity data
FROM open_deals a
LEFT JOIN activity_data b
ON a.partner_key = b.partner_key AND a.user_id = b.user_id
WHERE b.user_id IS NOT NULL -- Filter to include only matched records
