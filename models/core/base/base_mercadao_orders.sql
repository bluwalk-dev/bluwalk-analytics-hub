-- Main query to retrieve order details along with partner and user information
SELECT 
    b.partner_key,
    b.sales_partner_id,                  -- Partner ID from the partners accounts dimension
    b.partner_name,                -- Partner name from the partners accounts dimension
    c.contact_id,                  -- Contact ID from the users dimension
    c.user_id,                     -- User ID from the users dimension
    c.user_name,                        -- User's name from the users dimension
    a.partner_account_uuid,        -- Partner account UUID from the subquery 'a'
    a.date,                        -- Date of the order from the subquery 'a'
    a.nr_orders,                   -- Number of orders from the subquery 'a'
    a.order_type,                  -- Type of the order from the subquery 'a'
FROM ( 
    -- Subquery to get the latest order log for each partner account, date, and order type
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY partner_account_uuid, date, order_type 
            ORDER BY extraction_timestamp DESC
        ) AS __row_number
    FROM {{ ref("stg_mercadao__order_log") }} 
) a
LEFT JOIN {{ ref('dim_partners_accounts') }} b 
    ON a.partner_account_uuid = b.partner_account_uuid  -- Join with partners accounts dimension
LEFT JOIN {{ ref('dim_users') }} c 
    ON c.contact_id = b.contact_id                       -- Join with users dimension
WHERE 
    __row_number = 1 AND                                  -- Filter to get the latest record per group
    date < current_date                                  -- Filter to get records before the current date
ORDER BY date DESC                                       -- Order results by date in descending order