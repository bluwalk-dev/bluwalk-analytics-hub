select
    id hs_contact_id,
    hs_calculated_phone_number contact_phone_nr,
    odoo_user_id user_id,
    odoo_partner_id contact_id
from {{ ref("stg_hubspot__contacts") }} a
where is_deleted = FALSE