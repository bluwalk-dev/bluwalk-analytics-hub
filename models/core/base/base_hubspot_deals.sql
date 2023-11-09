SELECT DISTINCT
    a.deal_id,
    a.deal_pipeline_id,
    b.label deal_pipeline_name,
    a.deal_pipeline_stage_id,
    a.is_closed_won,
    a.is_closed,
    e.contact_id hs_contact_id,
    f.contact_id,
    f.user_id,
    a.owner_id hs_owner_id,
    IFNULL ( CONCAT(c.first_name, ' ', c.last_name), 'no-agent' ) owner_name,
    a.create_date,
    a.close_date
FROM {{ ref("stg_hubspot__deals") }} a
LEFT JOIN {{ ref("stg_hubspot__deal_pipelines") }} b on a.deal_pipeline_id = b.pipeline_id
LEFT JOIN {{ ref("stg_hubspot__owners") }} c on a.owner_id = c.owner_id
LEFT JOIN {{ ref("stg_hubspot__merged_deals") }} d on a.deal_id = d.merged_deal_id
LEFT JOIN {{ ref("stg_hubspot__deal_contacts") }} e on a.deal_id = e.deal_id
LEFT JOIN {{ ref("base_hubspot_contacts") }} f ON f.hs_contact_id = e.contact_id
WHERE is_deleted = FALSE AND d.deal_id IS NULL