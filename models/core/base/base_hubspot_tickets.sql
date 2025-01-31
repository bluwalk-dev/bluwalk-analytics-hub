WITH tickets AS (
    -- Your original ticket query
    SELECT
        'Hubspot' as ticket_system,
        'Bluwalk' as brand,
        cast(a.id as int64) as ticket_id,
        lower(a.property_source_type) as source,
        e.user_name agent_name,
        e.hubspot_team_name agent_team,
        d.contact_id,
        cast(a.property_odoo_user_id as int64) as user_id,
        d.user_location,
        cast(a.property_subject as string) as subject,
        lower(a.property_hs_ticket_priority) as priority,
        lower(cast(c.ticket_state as string)) as stage,
        cast(a.property_ticket_topic as string) as category,
        cast(a.property_createdate as datetime) as create_date,
        cast(a.property_closed_date as datetime) as close_date,
        cast(a.property_first_agent_reply_date as datetime) as first_reply,
        property_time_to_solve_working_hours / 360000 resolution_time
        
    from {{ ref("stg_hubspot__tickets") }} a
    left join {{ ref("stg_hubspot__ticket_pipeline_stages") }} c on a.property_hs_pipeline_stage = cast(c.stage_id as int)
    left join {{ ref("base_hubspot_users") }} e on CAST(a.property_hs_all_owner_ids AS INT64) = e.hubspot_owner_id
    left join {{ ref("dim_users") }} d on cast(a.property_odoo_user_id as int64) = d.user_id
    WHERE property_hs_pipeline = 0

    UNION ALL

    SELECT
        'Hubspot' as ticket_system,
        'Drivfit' as brand,
        cast(a.id as int64) as ticket_id,
        lower(a.property_source_type) as source,
        e.user_name agent_name,
        e.hubspot_team_name agent_team,
        NULL contact_id,
        NULL as user_id,
        NULL user_location,
        cast(a.property_subject as string) as subject,
        lower(a.property_hs_ticket_priority) as priority,
        lower(cast(c.ticket_state as string)) as stage,
        NULL as category,
        cast(a.property_createdate as datetime) as create_date,
        cast(a.property_closed_date as datetime) as close_date,
        cast(a.property_first_agent_reply_date as datetime) as first_reply,
        NULL resolution_time
        
    from {{ ref("stg_drivfit__hubspot_tickets") }} a
    left join {{ ref("stg_drivfit__hubspot_ticket_pipeline_stages") }} c on a.property_hs_pipeline_stage = cast(c.stage_id as int)
    left join {{ ref("base_hubspot_users") }} e on CAST(a.property_hs_all_owner_ids AS INT64) = e.hubspot_owner_id
    WHERE property_hs_pipeline = 0
),

-- Compute working hours per ticket
working_hours AS (
    SELECT 
        *,
        TIMESTAMP(create_date) AS start_time,
        TIMESTAMP(first_reply) AS end_time,
        TIMESTAMP(DATETIME(DATE(create_date), TIME(9, 0, 0))) AS work_start_start_day,
        TIMESTAMP(DATETIME(DATE(create_date), TIME(18, 0, 0))) AS work_end_start_day,
        TIMESTAMP(DATETIME(DATE(first_reply), TIME(9, 0, 0))) AS work_start_end_day,
        TIMESTAMP(DATETIME(DATE(first_reply), TIME(18, 0, 0))) AS work_end_end_day
    FROM tickets
),

time_computation AS (
    SELECT
        *,
        
        -- Case where start time is after 18:00 → Ignore the entire first day
        CASE 
            WHEN start_time >= work_end_start_day THEN 0  -- If start is after 18:00, no work counted on that day
            ELSE 
                CASE 
                    WHEN DATE(start_time) = DATE(end_time) 
                         AND EXTRACT(DAYOFWEEK FROM start_time) BETWEEN 2 AND 6 THEN 
                        CASE 
                            WHEN start_time < work_start_start_day THEN LEAST(TIMESTAMP_DIFF(LEAST(end_time, work_end_start_day), work_start_start_day, SECOND), 32400)
                            WHEN start_time > work_end_start_day THEN 0
                            ELSE LEAST(TIMESTAMP_DIFF(LEAST(end_time, work_end_start_day), start_time, SECOND), 32400)
                        END
                    ELSE 0
                END
        END AS same_day_seconds,

        -- First day working hours (only if it's a working day and starts before 18:00)
        CASE 
            WHEN DATE(start_time) <> DATE(end_time) 
                 AND EXTRACT(DAYOFWEEK FROM start_time) BETWEEN 2 AND 6 THEN
                CASE 
                    WHEN start_time >= work_end_start_day THEN 0  -- If start is after 18:00, ignore
                    WHEN start_time <= work_start_start_day THEN TIMESTAMP_DIFF(work_end_start_day, work_start_start_day, SECOND)
                    ELSE TIMESTAMP_DIFF(work_end_start_day, start_time, SECOND)
                END
            ELSE 0
        END AS first_day_seconds,

        -- Last day working hours (Only count from 09:00 to end_time)
        CASE 
            WHEN DATE(start_time) <> DATE(end_time) 
                 AND EXTRACT(DAYOFWEEK FROM end_time) BETWEEN 2 AND 6 THEN
                CASE 
                    WHEN end_time <= work_start_end_day THEN 0  -- If before work starts, ignore
                    WHEN end_time > work_end_end_day THEN TIMESTAMP_DIFF(work_end_end_day, work_start_end_day, SECOND) -- Full workday
                    ELSE TIMESTAMP_DIFF(end_time, work_start_end_day, SECOND) -- Only count time from 09:00 to end_time
                END
            ELSE 0
        END AS last_day_seconds,

        -- Count full working days in between (excluding weekends)
        CASE 
            WHEN DATE(start_time) = DATE(end_time) THEN 0  -- Same day: no full working days
            ELSE (SELECT COUNT(*)
                  FROM UNNEST(GENERATE_DATE_ARRAY(DATE(start_time) + 1, DATE(end_time) - 1)) AS d
                  WHERE EXTRACT(DAYOFWEEK FROM d) BETWEEN 2 AND 6) * 32400
        END AS full_working_seconds

    FROM working_hours
)

-- Final output with working_seconds column
SELECT 
    ticket_system,
    brand,
    ticket_id,
    source,
    agent_name,
    agent_team,
    contact_id,
    user_id,
    user_location,
    subject,
    priority,
    stage,
    category,
    create_date,
    close_date,
    first_reply,
    resolution_time,
    CASE
        WHEN first_reply IS NULL THEN NULL
        ELSE (same_day_seconds + first_day_seconds + last_day_seconds + full_working_seconds) / 3600 
    END AS first_reply_time
FROM time_computation
ORDER BY create_date DESC