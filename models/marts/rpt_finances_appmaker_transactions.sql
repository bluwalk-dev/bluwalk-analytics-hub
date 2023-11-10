SELECT
    a.vat,
    a.code,
    a.description,
    a.amount,
    a.payment_cycle,
    a.driver_operation_id,
    b.doc_url
FROM {{ ref('stg_google_appmaker__drivers_ledger') }} a
LEFT JOIN {{ ref('stg_google_appmaker__drivers_statement') }} b 
    ON a.payment_cycle = b.payment_cycle AND
    a.vat = b.vat
WHERE a.vat IS NOT NULL
ORDER BY payment_cycle ASC