SELECT
    a.call_system,
    a.call_uuid,
    a.direction,
    a.outcome,
    a.start_time, 
    a.end_time, 
    a.contact_phone_nr,
    CAST(c.property_odoo_partner_id AS INT64) contact_id,
    CAST(c.property_odoo_user_id AS INT64) user_id,
    a.duration,
    a.internal_line_nr, 
    a.internal_line_name,
    a.agent_email,
    b.employee_team agent_team,
    a.recording_link
FROM (
    SELECT * FROM {{ ref('base_talkdesk_calls') }}
    UNION ALL 
    SELECT * FROM {{ ref('base_aircall_calls') }}
) a
LEFT JOIN {{ ref('dim_employees') }} b ON a.agent_email = b.employee_email
LEFT JOIN {{ ref('stg_hubspot__contacts') }} c ON a.contact_phone_nr = c.property_hs_calculated_phone_number
ORDER BY a.end_time DESC