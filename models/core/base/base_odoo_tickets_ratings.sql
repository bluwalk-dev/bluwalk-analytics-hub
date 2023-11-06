SELECT DISTINCT
    'odoo' feedback_system,
    CAST(st.date_create AS DATETIME) AS date_time,
    st.id ticket_id,
    u.user_id,
    u.contact_id,
    e.employee_user_id agent_employee_id,
    e.employee_short_name agent_name,
    CAST(customer_rating as STRING) original_score,
    CAST(CAST(customer_rating AS INT64)/4 AS NUMERIC) as normalized_score,
    comment feedback_comment
FROM {{ ref('stg_odoo__support_tickets') }} st
LEFT JOIN {{ ref('dim_users__version2') }} u ON st.partner_id = u.contact_id
LEFT JOIN {{ ref('dim_employees__version2') }} e ON st.responsible_id = e.employee_user_id
WHERE customer_rating is not null