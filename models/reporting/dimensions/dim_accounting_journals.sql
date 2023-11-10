SELECT
    id journal_id,
    name journal_name,
    code journal_code,
    active journal_active,
    type journal_type
FROM {{ ref('stg_odoo__account_journals') }} a