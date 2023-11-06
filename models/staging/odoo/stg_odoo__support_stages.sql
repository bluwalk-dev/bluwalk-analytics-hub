select
    *
from {{ source('odoo_realtime', 'support_stage') }}