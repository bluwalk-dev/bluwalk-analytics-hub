WITH base_hubspot_contacts AS (
    WITH merged_objects AS (
        SELECT CAST(value AS INT64) value
        FROM 
            {{ ref("stg_hubspot_drivfit__contacts") }},
            UNNEST(SPLIT(merged_objects, ';')) AS value
        WHERE merged_objects IS NOT NULL
    )

    SELECT
        hs_contact_id,
        first_name,
        last_name,
        email,
        contact_phone_nr
        
    FROM {{ ref("stg_hubspot_drivfit__contacts") }}
    WHERE 
        is_deleted = FALSE AND
        hs_contact_id NOT IN (SELECT * FROM merged_objects)

)


-- Main query to retrieve detailed information about deals
SELECT DISTINCT
    a.deal_id,                            -- Deal ID from the hubspot deals table
    a.deal_name,                          -- Deal name from the hubspot deals table
    a.deal_pipeline_id,                   -- Deal pipeline ID from the hubspot deals table
    b.label as deal_pipeline_name,        -- Pipeline name joined from deal pipelines table
    a.deal_pipeline_stage_id,             -- Deal pipeline stage ID from the hubspot deals table
    a.create_date as create_datetime,     -- Create date and time of the deal
    i.date as create_date,                -- Create date from calendar table
    i.year_week as create_year_week,      -- Year and week of deal creation from calendar table
    i.year_month as create_year_month,    -- Year and month of deal creation from calendar table
    a.close_date as close_datetime,       -- Close date and time of the deal
    h.date as close_date,                 -- Close date from calendar table
    h.year_week as close_date_week,       -- Year and week of deal closure from calendar table
    h.year_month as close_date_month,     -- Year and month of deal closure from calendar table
    a.is_closed_won,                      -- Indicator if the deal is closed and won
    a.is_closed,                          -- Indicator if the deal is closed
    e.contact_id as hs_contact_id,        -- HubSpot contact ID from deal contacts table
    a.owner_id as hs_owner_id,            -- HubSpot owner ID from the hubspot deals table
    IFNULL(CONCAT(c.first_name, ' ', c.last_name), 'no-agent') as owner_name, -- Owner's name or 'no-agent' if not available
    a.deal_value,
    a.deal_team_bonus

FROM {{ ref("stg_hubspot_drivfit__deals") }} a
LEFT JOIN {{ ref("stg_hubspot_drivfit__deal_pipelines") }} b ON a.deal_pipeline_id = b.pipeline_id
LEFT JOIN {{ ref("stg_hubspot_drivfit__owners") }} c ON a.owner_id = c.owner_id
LEFT JOIN {{ ref("stg_hubspot_drivfit__merged_deals") }} d ON a.deal_id = d.merged_deal_id
LEFT JOIN {{ ref("stg_hubspot_drivfit__deal_contacts") }} e ON a.deal_id = e.deal_id
LEFT JOIN base_hubspot_contacts f ON f.hs_contact_id = e.contact_id
LEFT JOIN {{ ref("util_calendar") }} h ON CAST(a.close_date AS DATE) = h.date
LEFT JOIN {{ ref("util_calendar") }} i ON CAST(a.create_date AS DATE) = i.date
WHERE a.is_deleted = FALSE AND d.deal_id IS NULL  -- Filters for non-deleted and non-merged deals
ORDER BY a.create_date DESC                        -- Orders the results by create date in descending order