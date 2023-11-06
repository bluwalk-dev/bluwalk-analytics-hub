select
    *
from {{ source('odoo_realtime', 'booking') }}