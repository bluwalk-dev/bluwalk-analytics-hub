-- res_partner_enriched model
-- This model enriches the res_partner data from the stg_odoo__res_partners source
-- by joining it with the dim_locations dimension table. It provides a comprehensive
-- view of contact information including location details.

SELECT 
    -- Basic contact information
    rp.id as contact_id,  -- Unique identifier for the contact
    rp.name as contact_full_name,  -- Full name of the contact
    rp.friendly_name as contact_short_name,  -- Short or informal name
    rp.email_normalized as contact_email,  -- Normalized email address
    rp.lang as contact_language,  -- Preferred language of the contact
    rp.tz as contact_timezone,  -- Time zone information
    rp.street as contact_address,  -- Street address
    rp.zip as contact_zip_code,  -- Postal or ZIP code
    rp.city as contact_city,  -- City name
    rp.birthday as contact_birthdate,  -- Birthdate of the contact
    rp.gender as contact_gender,  -- Gender of the contact

    -- Location information joined from dim_locations
    l.location_name AS contact_location,  -- Name of the location
    l.location_country AS contact_location_country,  -- Country of the location

    -- Referral information
    rp.referral_code contact_referral_code,
    rp.referrer_code contact_referrer_code,

    -- Additional contact information
    rp.vat as contact_vat,  -- VAT number if applicable
    -- Phone number processing: Adding country code prefix based on the length or content of the phone number
    concat(if(length(rp.phone_sanitized)=9,'+351', if(left(rp.phone_sanitized,3)='351','+','')),rp.phone_sanitized) as contact_phone,
    rp.create_date as contact_create_date  -- Date when the contact was created
    
FROM {{ ref('stg_odoo__res_partners') }} rp  -- Source table: staged res partners data
LEFT JOIN {{ ref('dim_locations') }} l on rp.operation_city_id = l.location_id  -- Joining with location dimension table
WHERE rp.active is true  -- Filtering only active contacts
ORDER BY contact_id DESC  -- Ordering by contact_id in descending order for recent entries first