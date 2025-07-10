SELECT
  a.id as contact_id,
  a.name as contact_name,
  a.email as contact_email,
  a.mobile as contact_phone,
  a.vat as contact_vat,
  a.street as contact_address,
  a.zip as contact_zip,
  a.city as contact_city,
  c.name as contact_state,
  b.name as contact_country,
  a.driver_license_number as contact_driver_license_nr,
  a.citizen_card as contact_citizen_card_nr,
  a.parent_id as contact_parent_id
FROM {{ ref('stg_odoo_drivfit__res_partners') }} a
LEFT JOIN {{ ref('stg_odoo_drivfit__res_countries') }} b ON a.country_id = b.id
LEFT JOIN {{ ref('stg_odoo_drivfit__res_country_states') }} c ON a.state_id = c.id