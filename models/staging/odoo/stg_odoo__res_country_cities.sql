select
    *
from {{ source('odoo_realtime', 'res_country_city') }}