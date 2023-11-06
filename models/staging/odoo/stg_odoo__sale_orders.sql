select
    *
from {{ source('odoo_realtime', 'sale_order') }}