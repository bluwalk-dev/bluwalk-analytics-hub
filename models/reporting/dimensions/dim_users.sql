SELECT 
    ru.id as user_id,
    e.contact_id,
    ru.active,
    ru.login,
    e.short_name as name,
    e.phone,
    e.email,
    e.vat,
    e.language,
    e.timezone,
    e.address,
    e.zip_code,
    e.city,
    e.birthdate,
    e.gender,
    e.location,
    e.location_country,
    ru.create_date
FROM {{ ref('stg_odoo__res_users') }} ru
LEFT JOIN {{ ref('dim_contacts') }} e on ru.partner_id = e.contact_id
WHERE ru.active is true and e.contact_id is not null
ORDER BY ru.create_date DESC