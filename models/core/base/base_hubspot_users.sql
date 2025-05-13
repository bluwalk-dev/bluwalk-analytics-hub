SELECT
    'Bluwalk' as portal,
    a.id hubspot_user_id,
    d.first_name,
    d.last_name,
    concat(d.first_name, ' ', d.last_name) user_name,
    a.email,
    c.id hubspot_team_id,
    c.name hubspot_team_name,
    d.owner_id hubspot_owner_id
FROM {{ ref("stg_hubspot__users") }} a
LEFT JOIN {{ ref("stg_hubspot__teams") }} c on a.primary_team_id = c.id
LEFT JOIN {{ ref("stg_hubspot__owners") }} d on a.email = d.email

UNION ALL

SELECT
    'Drivfit' as portal,
    a.id hubspot_user_id,
    d.first_name,
    d.last_name,
    concat(d.first_name, ' ', d.last_name) user_name,
    a.email,
    c.id hubspot_team_id,
    c.name hubspot_team_name,
    d.owner_id hubspot_owner_id
FROM {{ ref("stg_hubspot_drivfit__users") }} a
LEFT JOIN {{ ref("stg_hubspot_drivfit__teams") }} c on a.primary_team_id = c.id
LEFT JOIN {{ ref("stg_hubspot_drivfit__owners") }} d on a.email = d.email