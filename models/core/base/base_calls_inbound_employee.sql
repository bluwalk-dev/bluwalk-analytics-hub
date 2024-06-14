SELECT
    b.call_uuid,
    b.direction,
    b.created_at,
    a.ringing_at,
    c.answered_at,
    b.ended_at,
    a.employee_email,
    CASE
        WHEN a.employee_email IN ('ssilva@bluwalk.com', 'pribeiro@bluwalk.com', 'acavalcante@bluwalk.com', 'cmiguel@bluwalk.com', 'joliveira@bluwalk.com') THEN 'Service'
        ELSE 'Activation'
    END team
FROM {{ ref('int_aircall_call_ringing') }} a
LEFT JOIN {{ ref('base_calls') }} b ON a.call_uuid = b.call_uuid
LEFT JOIN {{ ref('int_aircall_call_answered') }} c ON a.call_uuid = c.call_uuid AND a.employee_email = c.employee_email
ORDER BY created_at DESC
