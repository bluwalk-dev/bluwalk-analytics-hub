SELECT
    a.id as document_id,
    a.document_date,
    a.name as document_name,
    concat(a.direction, '_', a.type) as document_type,
    a.status as document_status,
    a.issued_by_id as issuer_contact_id,
    a.financial_system_id as system_id,
    b.name as system_name,
    a.external_id as system_move_id,
    a.transaction_account_id,
    a.amount,
    a.amount_total,
    a.amount_residual,
    a.last_sync_time
FROM bluwalk-analytics-hub.staging.stg_odoo_bw_financial_documents a
LEFT JOIN {{ ref('stg_odoo__financial_systems') }} b ON a.financial_system_id = b.id