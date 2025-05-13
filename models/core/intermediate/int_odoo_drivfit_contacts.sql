SELECT
  id as contact_id,
  name as contact_name,
  email as contact_email,
  mobile as contact_phone,
  vat as contact_vat,
  street as contact_address,
  zip as contact_zip,
  city as contact_city
FROM {{ ref('stg_odoo_drivfit__res_partners') }}