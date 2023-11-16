SELECT DISTINCT
    a.deal_id,
    a.deal_pipeline_id,
    b.label deal_pipeline_name,
    IFNULL(a.fuel_energy_partner_name, IF(a.training_tvde_partner_name = 'false', 'ACP', a.training_tvde_partner_name)) partner_name,
    a.deal_pipeline_stage_id,
    a.create_date create_datetime,
    i.date create_date,
    i.year_week create_year_week,
    i.year_month create_year_month,
    a.close_date close_datetime,
    h.date close_date,
    h.year_week close_date_week,
    h.year_month close_date_month,
    a.is_closed_won,
    a.is_closed,
    e.contact_id hs_contact_id,
    f.contact_id,
    f.user_id,
    a.owner_id hs_owner_id,
    IFNULL ( CONCAT(c.first_name, ' ', c.last_name), 'no-agent' ) owner_name,
    k.activation_team owner_team,
    k.marketing_point_score,
    j.activation_point_score
FROM {{ ref("stg_hubspot__deals") }} a
LEFT JOIN {{ ref("stg_hubspot__deal_pipelines") }} b on a.deal_pipeline_id = b.pipeline_id
LEFT JOIN {{ ref("stg_hubspot__owners") }} c on a.owner_id = c.owner_id
LEFT JOIN {{ ref("stg_hubspot__merged_deals") }} d on a.deal_id = d.merged_deal_id
LEFT JOIN {{ ref("stg_hubspot__deal_contacts") }} e on a.deal_id = e.deal_id
LEFT JOIN {{ ref("base_hubspot_contacts") }} f ON f.hs_contact_id = e.contact_id
LEFT JOIN {{ ref("util_calendar") }} h ON CAST(a.close_date AS DATE) = h.date
LEFT JOIN {{ ref("util_calendar") }} i ON CAST(a.create_date AS DATE) = i.date
LEFT JOIN {{ ref("stg_hubspot__pipelines") }} j ON j.pipeline_id = a.deal_pipeline_id AND h.year_quarter = j.year_quarter
LEFT JOIN {{ ref("stg_hubspot__pipelines") }} k ON k.pipeline_id = a.deal_pipeline_id AND i.year_quarter = k.year_quarter
WHERE is_deleted = FALSE AND d.deal_id IS NULL
ORDER BY create_date DESC