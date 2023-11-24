SELECT
    analytic_account_owner_contact_id contact_id,
    analytic_account_id
FROM
{{ ref('dim_accounting_analytic_accounts') }}
WHERE 
    analytic_account_type = 'User' AND
    analytic_account_state = 'active'