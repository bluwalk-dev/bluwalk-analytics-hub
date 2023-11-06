select
    *
from {{ source('odoo_realtime', 'hr_employee') }}