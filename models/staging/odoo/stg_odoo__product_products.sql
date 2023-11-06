select
    *
from {{ source('odoo_realtime', 'product_product') }}