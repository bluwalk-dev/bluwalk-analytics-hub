SELECT 
    rp.id as contact_id,
    rp.name as full_name,
    rp.friendly_name as short_name,
    rp.email_normalized as email,
    rp.lang as language,
    rp.tz as timezone,
    rp.street as address,
    rp.zip as zip_code,
    rp.city as city,
    rp.birthday birthdate,
    rp.gender as gender,
    l.name AS location,
    l.country AS location_country,
    rp.vat as vat,
    concat(if(length(rp.phone_sanitized)=9,'+351', if(left(rp.phone_sanitized,3)='351','+','')),rp.phone_sanitized) as phone,
    rp.create_date as create_date
    
FROM {{ ref('stg_odoo__res_partners') }} rp
LEFT JOIN {{ ref('dim_locations__version2') }} l on rp.operation_city_id = l.location_id
WHERE rp.active is true
ORDER BY contact_id DESC