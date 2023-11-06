select
    *
from {{ source('odoo_realtime', 'res_users') }}