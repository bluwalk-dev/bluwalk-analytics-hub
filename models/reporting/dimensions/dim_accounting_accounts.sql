SELECT
    id,
    name,
    code,
    deprecated,
    internal_type,
    internal_group
FROM {{ ref('stg_odoo__account_accounts') }} a