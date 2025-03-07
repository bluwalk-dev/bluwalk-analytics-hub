SELECT a.document_id
FROM {{ ref('fct_financial_documents') }} a
LEFT JOIN {{ ref('fct_accounting_moves') }} b ON a.system_id = b.financial_system_id AND a.system_move_id = b.id
WHERE b.id IS NULL AND document_status NOT IN ('cancelled', 'pending')