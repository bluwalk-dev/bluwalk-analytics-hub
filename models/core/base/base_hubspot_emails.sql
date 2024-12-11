SELECT
    a.create_ts,
    a.create_local_time,
    a.hubspot_owner_id,
    b.portal as agent_portal,
    b.email as agent_email,
    b.user_name as agent_name,
    b.hubspot_team_name as agent_team,
    a.email_direction,
    a.email_address_to,
    a.email_name_to,
    a.email_address_from,
    a.email_name_from
FROM {{ ref("stg_hubspot__engagement_emails") }} a
LEFT JOIN {{ ref("base_hubspot_users") }} b ON a.hubspot_owner_id = b.hubspot_owner_id
where b.hubspot_owner_id IS NOT NULL