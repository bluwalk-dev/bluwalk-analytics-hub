select
    *
from {{ source('odoo_realtime', 'fuel_card_log') }}