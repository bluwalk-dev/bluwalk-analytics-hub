SELECT
    a.vehicle_contract_id rental_id,
    a.vehicle_contract_name rental_name,
    a.vehicle_plate vehicle,
    a.start_date,
    a.end_date,
    initcap(e.contact_name) client,
    e.contact_vat client_vat,
    a.driver_id,
    initcap(c.contact_name) name,
    c.contact_vat vat,
    initcap(c.contact_address) address,
    c.contact_zip zip,
    initcap(c.contact_city) city,
    initcap(c.contact_state) district,
    initcap(c.contact_country) country,
    c.contact_phone mobile,
    lower(c.contact_email) email,
    c.contact_driver_license_nr drivers_license,
    c.contact_citizen_card_nr citizen_card,
from {{ ref('fct_fleet_rental_contracts') }} a
left join {{ ref('int_odoo_drivfit_contacts') }} c on a.driver_id = c.contact_id
left join {{ ref('int_odoo_drivfit_contacts') }} e on a.customer_id = e.contact_id
order by 
    if(a.end_date is null, 2,1) desc, 
    a.start_date desc, 
    a.end_date desc