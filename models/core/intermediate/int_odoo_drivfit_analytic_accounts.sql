WITH vehicle_accounts AS (
    SELECT
        analytic_account_id,
        vehicle_id
    FROM {{ ref('dim_fleet_vehicles') }}
    WHERE analytic_account_id IS NOT NULL
)

SELECT
    id AS analytic_account_id, -- Unique identifier for the analytic account
    name AS analytic_account_name, -- Name of the analytic account
    group_id AS analytic_account_group_id, -- Group ID associated with the analytic account
    NULL AS analytic_account_group, -- Placeholder for future group name, currently not available
    partner_id AS analytic_account_owner_contact_id, -- Contact ID for the owner of the analytic account
    CASE
        WHEN name LIKE 'BA/%' THEN 'Customer' -- Categorizing as 'User' type for names starting with 'BA/'
        WHEN name LIKE 'FLEET/%' THEN 'Vehicle'
        ELSE 'Accounting' -- Default category for other account names
    END AS analytic_account_type,
    b.vehicle_id,
    state AS analytic_account_state -- Current state of the account
FROM {{ ref('stg_odoo_drivfit__account_analytic_accounts') }} a
LEFT JOIN vehicle_accounts b ON a.id = b.analytic_account_id
WHERE active IS TRUE -- Filtering to include only active accounts