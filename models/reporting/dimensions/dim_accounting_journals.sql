SELECT
    id,
    name,
    code,
    active,
    type
FROM {{ ref('stg_odoo__account_journals') }} a