select
    'hubspot' as ticket_system,
    cast(a.id as int64) as ticket_id,
    lower(a.property_source_type) as source,
    e.employee_short_name agent_name,
    e.employee_hubspot_team agent_team,
    d.contact_id,
    cast(a.property_odoo_user_id as int64) as user_id,
    d.user_location,
    cast(a.property_subject as string) as subject,
    lower(a.property_hs_ticket_priority) as priority,
    lower(cast(c.ticket_state as string)) as stage,
    cast(a.property_ticket_topic as string) as category,
    cast(a.property_createdate as datetime) as create_date,
    cast(a.property_closed_date as datetime) as close_date,
    property_time_to_solve_working_hours / 360000 resolution_time,
    property_time_to_first_agent_reply_wh / 3600000 first_reply_time
    
from {{ ref("stg_hubspot__tickets") }} a
left join {{ ref("stg_hubspot__ticket_pipeline_stages") }} c on a.property_hs_pipeline_stage = cast(c.stage_id as int)
left join {{ ref("dim_employees") }} e on CAST(a.property_hs_all_owner_ids AS INT64) = e.employee_hubspot_owner_id
left join {{ ref("dim_users") }} d on cast(a.property_odoo_user_id as int64) = d.user_id
WHERE property_hs_pipeline = 0
order by create_date DESC
