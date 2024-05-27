SELECT 
    a.id
FROM {{ ref("stg_drivfit__hubspot_contacts") }} a
LEFT JOIN {{ ref("dim_users") }} b ON a.property_email = b.user_email
WHERE 
    b.user_id IS NOT NULL AND
    a.property_bluwalk_user IS NULL