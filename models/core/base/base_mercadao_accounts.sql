SELECT 
    b.partner_id,
    b.partner_name,
    c.contact_id,
    c.user_id,
    c.name,
    a.partner_account_uuid,
    a.account_status,
    a.shopper_email,
    a.shopper_phone_number,
    a.extraction_timestamp AS last_update_ts
FROM ( 
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY partner_account_uuid ORDER BY extraction_timestamp DESC) AS __row_number
    FROM {{ ref("stg_mercadao__account_status_log") }} ) a
LEFT JOIN {{ ref('dim_partners_accounts') }} b on a.partner_account_uuid = b.partner_account_uuid
LEFT JOIN {{ ref('dim_users') }} c on c.contact_id = b.contact_id
WHERE  __row_number = 1
ORDER BY a.partner_account_uuid DESC