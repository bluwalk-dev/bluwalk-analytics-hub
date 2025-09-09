SELECT 
    b.partner_key,
    b.sales_partner_id,
    b.partner_name,
    c.contact_id,
    c.user_id,
    c.user_name,
    a.partner_account_uuid,
    a.account_status,
    a.shopper_email,
    a.shopper_phone_number,
    a.extraction_timestamp AS last_update_ts
FROM bluwalk-analytics-hub.staging.stg_mercadao_account_status a
LEFT JOIN {{ ref('dim_partners_accounts') }} b on a.partner_account_uuid = b.partner_account_uuid
LEFT JOIN {{ ref('dim_users') }} c on c.contact_id = b.contact_id
ORDER BY a.partner_account_uuid DESC