SELECT 
    b.partner_id,
    b.partner_name,
    c.contact_id,
    c.user_id,
    c.name,
    a.partner_account_uuid,
    a.date,
    a.nr_orders,
    a.order_type,
FROM ( 
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY partner_account_uuid, date, order_type ORDER BY extraction_timestamp DESC) AS __row_number
    FROM {{ ref("stg_mercadao__order_log") }} ) a
LEFT JOIN {{ ref('dim_partners_accounts') }} b on a.partner_account_uuid = b.partner_account_uuid
LEFT JOIN {{ ref('dim_users') }} c on c.contact_id = b.contact_id
WHERE 
    __row_number = 1 AND
    date < current_date
ORDER BY date DESC