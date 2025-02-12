SELECT 
    
    rp.id as accounting_contact_id,
    rp.name as accounting_contact_name,  -- Full name of the contact
    rp.email_normalized as accounting_contact_email,  -- Normalized email address
    rp.lang as accounting_contact_language,  -- Preferred language of the contact
    rp.tz as accounting_contact_timezone,  -- Time zone information
    rp.street as accounting_contact_address,  -- Street address
    rp.zip as accounting_contact_zip_code,  -- Postal or ZIP code
    rp.city as accounting_contact_city,  -- City name
    rp.is_company as accounting_contact_is_company,
    rp.vat as accounting_contact_vat,  -- VAT number if applicable
    rp.create_date as accounting_contact_create_date  -- Date when the contact was created
    
FROM {{ ref('stg_odoo_enterprise__res_partners') }} rp  -- Source table: staged res partners data
WHERE 
    rp.active is true