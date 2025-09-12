SELECT 
    a.id as payment_profile_id,
    a.driver_id as payment_profile_contact_id,
    c.user_id as payment_profile_user_id,
    a.partner_id as payment_profile_payee_contact_id,
    a.state,
    a.payout_frequency,
    b.contact_is_company as payee_is_company
FROM bluwalk-analytics-hub.staging.stg_odoo_bw_payment_profiles a
LEFT JOIN bluwalk-analytics-hub.core.core_contacts_bw b ON a.partner_id = b.contact_id
LEFT JOIN bluwalk-analytics-hub.core.core_users c ON a.driver_id = c.contact_id