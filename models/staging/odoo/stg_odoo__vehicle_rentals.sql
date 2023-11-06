select
    *
from {{ source('odoo_realtime', 'vehicle_rental') }}