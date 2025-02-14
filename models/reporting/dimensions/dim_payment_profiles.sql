SELECT 
    a.id as payment_profile_id,
    a.driver_id as payment_profile_contact_id,
    c.user_id as payment_profile_user_id,
    a.partner_id as payment_profile_payee_contact_id,
    a.state,
    a.payout_frequency,
    b.contact_is_company as payee_is_company
FROM {{ ref('stg_odoo__payment_profiles') }} a
LEFT JOIN {{ ref('dim_contacts') }} b ON a.partner_id = b.contact_id
LEFT JOIN {{ ref('dim_users') }} c ON a.driver_id = c.contact_id