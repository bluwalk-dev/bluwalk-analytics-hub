select
    *
from {{ source('odoo_realtime', 'close_period') }}