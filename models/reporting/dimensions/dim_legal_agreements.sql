SELECT 
    a.id as legal_agreement_id,
    COALESCE(a.partner_id, d.driver_id) as contact_id,
    c.user_id,
    b.name as legal_agreement_template_name,
    a.report_id as legal_agreement_template_id,
    a.state as legal_agreement_state,
    a.create_date as legal_agreement_created_timestamp,
    a.accepted_timestamp as legal_agreement_accepted_timestamp
FROM {{ ref('stg_odoo__contract_type_lines') }} a
LEFT JOIN {{ ref('stg_odoo__xml_reports') }} b on a.report_id = b.id
LEFT JOIN {{ ref('dim_users') }} c on a.partner_id = c.contact_id
LEFT JOIN {{ ref('stg_odoo__payment_profiles') }} d on a.payment_profile_id = d.id
ORDER BY a.id DESC
