select
  a.id hubspot_user_id,
  concat(d.first_name, ' ', d.last_name) user_name,
  a.email,
  c.id hubspot_team_id,
  c.name hubspot_team_name,
  d.owner_id hubspot_owner_id
from {{ ref("stg_hubspot__users") }} a
left join {{ ref("stg_hubspot__team_users") }} b on a.id = b.user_id
left join {{ ref("stg_hubspot__teams") }} c on a.primary_team_id = c.id
left join {{ ref("stg_hubspot__owners") }} d on a.email = d.email