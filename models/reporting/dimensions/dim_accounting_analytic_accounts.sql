-- This model creates a table for accounting analytic accounts, transforming data from the stg_odoo__account_analytic_accounts source table.
-- It includes basic account information along with a custom field to categorize accounts based on their name.

SELECT
    id AS analytic_account_id, -- Unique identifier for the analytic account
    name AS analytic_account_name, -- Name of the analytic account
    group_id AS analytic_account_group_id, -- Group ID associated with the analytic account
    NULL AS analytic_account_group, -- Placeholder for future group name, currently not available
    partner_id AS analytic_account_owner_contact_id, -- Contact ID for the owner of the analytic account
    CASE
        WHEN (name LIKE 'BA/%' AND contract_type_id = 2) THEN 'User' -- Categorizing as 'User' type for names starting with 'BA/'
        WHEN (name LIKE 'BA/%' AND contract_type_id != 2) THEN 'Deprecated'
        ELSE 'Accounting' -- Default category for other account names
    END AS analytic_account_type,
    state AS analytic_account_state -- Current state of the account
FROM {{ ref('stg_odoo__account_analytic_accounts') }}
WHERE active IS TRUE -- Filtering to include only active accounts