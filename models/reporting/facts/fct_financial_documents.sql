SELECT
    a.document_id,
    a.document_date,
    a.document_name,
    a.document_type,
    a.document_status,
    a.issuer_contact_id,
    a.system_id,
    b.system_name,
    a.system_move_id,
    a.transaction_account_id,
    a.amount,
    a.amount_total,
    a.amount_residual,
    a.last_sync_time
FROM bluwalk-analytics-hub.staging.stg_odoo_bw_financial_documents a
LEFT JOIN {{ ref('stg_odoo__financial_systems') }} b ON a.financial_system_id = b.id