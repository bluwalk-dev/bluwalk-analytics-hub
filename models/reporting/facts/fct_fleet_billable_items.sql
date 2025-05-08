SELECT
    id,
    name,
    status,
    vehicle_id,
    customer_id,
    product_id,
    tax_id,
    description,
    CASE 
        WHEN rc_contract_id IS NOT NULL THEN rc_contract_id
        ELSE opl_contract_id
    END as contract_id,
    lease_contract_conditions_id as opl_conditions_id,
    CASE 
        WHEN rc_contract_id IS NOT NULL THEN 'short-term'
        ELSE 'mid-term'
    END as contract_type,
    contract_name,
    period_start,
    period_end,
    quantity,
    amount,
    external_invoice_id,
    external_invoice_line_id,
    last_sync,
    payment_cycle
FROM {{ ref('stg_odoo_drivfit__billable_items') }} a