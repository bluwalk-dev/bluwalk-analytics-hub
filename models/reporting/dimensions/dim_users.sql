/*
  user_enriched model
  This model enriches the user data from the stg_odoo__res_users source
  by joining it with the dim_contacts dimension table. It provides a comprehensive
  view of user details, including contact information and account status.

  Source Tables:
  - stg_odoo__res_users: Contains basic user account information.
  - dim_contacts: Contains detailed contact information.
*/

SELECT 
    ru.id as user_id,  -- Unique identifier for the user
    e.contact_id,  -- Contact identifier associated with the user
    ru.active as user_active,  -- Flag indicating if the user account is active
    ru.login as user_login,  -- Login name of the user
    e.contact_short_name as user_name,  -- Short name of the user's contact
    e.contact_phone as user_phone,  -- Phone number of the user
    e.contact_email as user_email,  -- Email address of the user
    e.contact_vat as user_vat,  -- VAT number associated with the user
    e.contact_language as user_language,  -- Preferred language of the user
    e.contact_timezone as user_timezone,  -- Timezone of the user
    e.contact_address as user_address,  -- Address of the user
    e.contact_zip_code as user_zip_code,  -- Zip code of the user's address
    e.contact_city as user_city,  -- City of the user's address
    e.contact_birthdate as user_birthdate,  -- Birthdate of the user
    e.contact_gender as user_gender,  -- Gender of the user
    e.contact_location as user_location,  -- Location name of the user
    e.contact_location_country as user_location_country,  -- Country of the user's location
    ru.create_date  -- Date when the user account was created

FROM {{ ref('stg_odoo__res_users') }} ru  -- Source table: staged res users data
LEFT JOIN {{ ref('dim_contacts') }} e ON ru.partner_id = e.contact_id  -- Joining with contacts dimension table for detailed contact information
WHERE ru.active IS TRUE AND e.contact_id IS NOT NULL  -- Filtering only active users with associated contact information
ORDER BY ru.create_date DESC  -- Ordering by account creation date in descending order