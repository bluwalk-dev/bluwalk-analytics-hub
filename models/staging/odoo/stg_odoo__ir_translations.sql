select
    *
from {{ source('odoo_realtime', 'ir_translation') }}