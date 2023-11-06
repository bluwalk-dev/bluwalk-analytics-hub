SELECT
    a.account_unique_id AS partner_account_uuid,
    a.partner_id AS contact_id,
    c.user_id,
    a.res_sales_partner_id AS partner_id,
    b.partner_name,
    a.state,
    a.create_date
FROM {{ ref('stg_odoo__res_sales_partner_accounts') }} a
LEFT JOIN {{ ref('dim_partners__version2') }} b ON a.res_sales_partner_id = b.partner_id
LEFT JOIN {{ ref('dim_users__version2') }} c ON a.partner_id = c.contact_id