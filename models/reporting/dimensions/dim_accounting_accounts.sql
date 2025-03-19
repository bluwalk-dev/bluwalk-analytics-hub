SELECT
    key as account_key,
    financial_system_id as account_financial_system_id,
    financial_system as account_financial_system,
    id AS account_id,
    name AS account_name,
    code AS account_code,
    deprecated AS account_deprecated,
    internal_type AS account_internal_type,
    internal_group AS account_internal_group
FROM 
    {{ ref('stg_odoo__account_accounts') }} a

UNION ALL

SELECT
    key as account_key,
    financial_system_id as account_financial_system_id,
    financial_system as account_financial_system,
    id AS account_id,
    name AS account_name,
    code AS account_code,
    deprecated AS account_deprecated,
    NULL AS account_internal_type,
    internal_group AS account_internal_group
FROM 
    {{ ref('stg_odoo_enterprise__account_accounts') }} a