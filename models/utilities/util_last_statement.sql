SELECT 
    CAST(period AS INT64) last_statement
FROM {{ ref('stg_odoo__close_periods') }}