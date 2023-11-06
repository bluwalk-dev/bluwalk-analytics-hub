SELECT
    'hubspot' feedback_system,
    CAST(t.property_hs_feedback_last_survey_date AS DATETIME) date_time,
    CAST(t.id AS INT64) ticket_id,
    u.user_id,
    u.contact_id,
    e.employee_user_id agent_employee_id,
    e.employee_short_name agent_name,
    CAST(t.property_hs_feedback_last_ces_rating AS STRING) original_score,
    CAST((CAST(t.property_hs_feedback_last_ces_rating AS INT64) - 1) / 6 AS NUMERIC) normalized_score,
    CAST(t.property_hs_feedback_last_ces_follow_up AS STRING) feedback_comment
FROM {{ ref("stg_hubspot__tickets") }} t
LEFT JOIN {{ ref("dim_employees") }} e ON CAST(t.property_hs_all_owner_ids AS INT64) = e.employee_hubspot_owner_id
LEFT JOIN {{ ref('dim_users') }} u ON CAST(t.property_odoo_user_id AS INT64) = u.user_id
WHERE property_hs_feedback_last_survey_date is not null
