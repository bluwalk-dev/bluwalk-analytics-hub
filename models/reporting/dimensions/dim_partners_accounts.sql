SELECT
    LOWER(a.account_unique_id) AS partner_account_uuid,  -- Unique identifier for the partner account
    a.partner_id AS contact_id,  -- Contact identifier associated with the partner account
    c.user_id,  -- User ID associated with the contact
    a.res_sales_partner_id AS sales_partner_id,  -- Sales partner identifier
    b.partner_key,
    b.partner_name,  -- Name of the sales partner
    a.state AS partner_account_state,  -- Current state of the partner account
    a.create_date  -- Date when the partner account was created

FROM {{ ref('stg_odoo__res_sales_partner_accounts') }} a  -- Source table: staged sales partner accounts data
LEFT JOIN {{ ref('dim_partners') }} b ON a.res_sales_partner_id = b.sales_partner_id  -- Joining with partners dimension table for partner details
LEFT JOIN {{ ref('dim_users') }} c ON a.partner_id = c.contact_id  -- Joining with users dimension table for user details
WHERE partner_marketplace = 'Work'