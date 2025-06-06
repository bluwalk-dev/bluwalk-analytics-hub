SELECT 
    a.id as policy_payment_id,
    a.name as payment_name,
    a.policy_id as insurance_policy_id,
    b.insurance_policy_number,
    a.start_date as start_date,
    a.end_date as end_date,
    a.amount,
    a.payment_type as policy_payment_type,
    a.payment_document_nr as policy_payment_document_nr
FROM {{ ref('stg_odoo__insurance_policy_payments') }} a
LEFT JOIN {{ ref('dim_insurance_policies') }} b ON a.policy_id = b.insurance_policy_id