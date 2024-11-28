SELECT
    'Hubspot' feedback_system,
    'Bluwalk' brand,
    CAST(t.property_hs_feedback_last_survey_date AS DATETIME) date_time,
    CAST(t.id AS INT64) ticket_id,
    u.user_id,
    u.contact_id,
    e.user_name agent_name,
    e.hubspot_team_name agent_team,
    CAST(t.property_hs_feedback_last_ces_rating AS STRING) original_score,
    CAST((CAST(t.property_hs_feedback_last_ces_rating AS INT64) - 1) / 6 AS NUMERIC) normalized_score,
    CAST(t.property_hs_feedback_last_ces_follow_up AS STRING) feedback_comment
FROM {{ ref("stg_hubspot__tickets") }} t
LEFT JOIN {{ ref("base_hubspot_users") }} e ON CAST(t.property_hs_all_owner_ids AS INT64) = e.hubspot_owner_id
LEFT JOIN {{ ref('dim_users') }} u ON CAST(t.property_odoo_user_id AS INT64) = u.user_id
WHERE property_hs_feedback_last_survey_date is not null

UNION ALL

SELECT
    'Hubspot' feedback_system,
    'Drivfit' brand,
    CAST(t.property_hs_feedback_last_survey_date AS DATETIME) date_time,
    CAST(t.id AS INT64) ticket_id,
    NULL user_id,
    NULL contact_id,
    e.user_name agent_name,
    e.hubspot_team_name agent_team,
    CAST(t.property_hs_feedback_last_ces_rating AS STRING) original_score,
    CAST((CAST(t.property_hs_feedback_last_ces_rating AS INT64) - 1) / 6 AS NUMERIC) normalized_score,
    CAST(t.property_hs_feedback_last_ces_follow_up AS STRING) feedback_comment
FROM {{ ref("stg_drivfit__hubspot_tickets") }} t
LEFT JOIN {{ ref("base_hubspot_users") }} e ON CAST(t.property_hs_all_owner_ids AS INT64) = e.hubspot_owner_id
WHERE property_hs_feedback_last_survey_date is not null

ORDER BY date_time DESC