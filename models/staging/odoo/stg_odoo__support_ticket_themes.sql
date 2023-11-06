select
    *
from {{ source('odoo_realtime', 'support_ticket_theme') }}