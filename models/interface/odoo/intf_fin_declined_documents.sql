SELECT 
    a.id as financial_document_id,
    b.id as enterprise_move_id
FROM bluwalk-analytics-hub.staging.stg_odoo_bw_financial_documents a
LEFT JOIN {{ ref('stg_odoo_enterprise__account_moves') }} b ON a.external_id = b.id
WHERE
    a.financial_system_id = 4 AND
    b.state = 'cancel' AND
    a.status NOT IN ('declined')