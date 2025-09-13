-- Main query to retrieve detailed information about deals
SELECT DISTINCT
    a.deal_id,                            -- Deal ID from the hubspot deals table
    a.deal_name,                          -- Deal name from the hubspot deals table
    a.deal_pipeline_id,                   -- Deal pipeline ID from the hubspot deals table
    b.label as deal_pipeline_name,        -- Pipeline name joined from deal pipelines table
    a.deal_partner_key,
    j.partner_marketplace,                -- Partner marketplace joined from pipelines table
    j.partner_name,
    a.energy_card_name,
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
    f.contact_id,                         -- Contact ID from the base hubspot contacts table
    f.user_id,                            -- User ID from the base hubspot contacts table
    a.owner_id as hs_owner_id,            -- HubSpot owner ID from the hubspot deals table
    IFNULL(CONCAT(c.first_name, ' ', c.last_name), 'no-agent') as owner_name, -- Owner's name or 'no-agent' if not available
    a.vehicle_plate,                      -- Vehicle plate information from the hubspot deals table
    a.deal_value,

    -- Marketing Fields
    a.google_analytics_client_id,
    a.facebook_click_id,
    a.google_ad_click_id,
    a.latest_source,
    a.original_source,
    a.source_url,

    -- Personal Vehicle Fields
    a.personal_vehicle_lost_reason,

    -- Insurance Fields
    a.insurance_insurer_id,
    a.insurance_policy_type_id,
    a.insurance_policy_name,
    a.insurance_annual_premium,
    a.insurance_entered_open,
    a.insurance_exited_open,
    a.insurance_entered_accepted,
    a.insurance_exited_accepted,
    a.insurance_entered_proposal_sent,
    a.insurance_exited_proposal_sent,
    a.insurance_quote_fidelidade,
    a.insurance_quote_allianz,
    a.insurance_quote_lusitania,
    a.insurance_quote_tranquilidade,

    -- Drivfit Fields
    a.vehicle_rental_license_plate,
    a.vehicle_rental_rate,
    a.vehicle_rental_pickup_date,
    a.vehicle_rental_pickup_station,
    a.vehicle_rental_booking_type,

    -- Team Performance Fields
    k.activation_team as owner_team,      -- Activation team name from pipelines table
    k.marketing_point_score,              -- Marketing point score from pipelines table
    a.deal_value as activation_point_score              -- Activation point score from pipelines table

FROM bluwalk-analytics-hub.staging.stg_hubspot_deals a
LEFT JOIN {{ ref("stg_hubspot__deal_pipelines") }} b ON a.deal_pipeline_id = b.pipeline_id
LEFT JOIN {{ ref("stg_hubspot__owners") }} c ON a.owner_id = c.owner_id
LEFT JOIN {{ ref("stg_hubspot__merged_deals") }} d ON a.deal_id = d.merged_deal_id
LEFT JOIN {{ ref("stg_hubspot__deal_contacts") }} e ON a.deal_id = e.deal_id
LEFT JOIN {{ ref("base_hubspot_contacts") }} f ON f.hs_contact_id = e.contact_id
LEFT JOIN bluwalk-analytics-hub.core.ref_calendar h ON CAST(a.close_date AS DATE) = h.date
LEFT JOIN bluwalk-analytics-hub.core.ref_calendar i ON CAST(a.create_date AS DATE) = i.date
LEFT JOIN {{ ref("dim_partners") }} j ON j.partner_key = a.deal_partner_key
LEFT JOIN {{ ref("fct_quarter_params") }} k ON i.year_quarter = k.year_quarter  AND k.partner_key = a.deal_partner_key
WHERE a.is_deleted = FALSE AND d.deal_id IS NULL  -- Filters for non-deleted and non-merged deals
ORDER BY a.create_date DESC                        -- Orders the results by create date in descending order