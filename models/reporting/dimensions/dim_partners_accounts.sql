/*
  partner_account_enriched model
  This model enriches the sales partner account data from the stg_odoo__res_sales_partner_accounts source
  by joining it with the dim_partners and dim_users dimension tables. It provides a comprehensive
  view of partner accounts, including contact and user details, partner names, and account status.

  Source Tables:
  - stg_odoo__res_sales_partner_accounts: Contains basic sales partner account information.
  - dim_partners: Contains detailed partner information.
  - dim_users: Contains user information linked to contacts.
*/

SELECT
    a.account_unique_id AS partner_account_uuid,  -- Unique identifier for the partner account
    a.partner_id AS contact_id,  -- Contact identifier associated with the partner account
    c.user_id,  -- User ID associated with the contact
    a.res_sales_partner_id AS partner_id,  -- Sales partner identifier
    b.partner_name,  -- Name of the sales partner
    a.state AS partner_account_state,  -- Current state of the partner account
    a.create_date  -- Date when the partner account was created

FROM {{ ref('stg_odoo__res_sales_partner_accounts') }} a  -- Source table: staged sales partner accounts data
LEFT JOIN {{ ref('dim_partners') }} b ON a.res_sales_partner_id = b.partner_id  -- Joining with partners dimension table for partner details
LEFT JOIN {{ ref('dim_users') }} c ON a.partner_id = c.contact_id  -- Joining with users dimension table for user details