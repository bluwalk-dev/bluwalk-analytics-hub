select
    id account_id,
    name account_name,
    group_id account_group_id,
    NULL as account_group,
    partner_id account_owner_contact_id,
    CASE
        WHEN name LIKE 'BA/%' THEN 'User'
        ELSE 'Accounting'
    END account_type,
    state account_state
from {{ ref('stg_odoo__account_analytic_accounts') }} a
where active IS TRUE