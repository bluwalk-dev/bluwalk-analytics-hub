SELECT DISTINCT 
    dc.deal_id, 
    c.property_email 
FROM {{ ref("stg_drivfit__hubspot_deal_contacts") }} dc
LEFT JOIN {{ ref("stg_drivfit__hubspot_contacts") }} c on dc.contact_id = c.id
LEFT JOIN {{ ref("stg_drivfit__hubspot_deals") }} d on dc.deal_id = d.deal_id
WHERE 
    d.deal_pipeline_id = '320984512' AND 
    d.is_deleted = false