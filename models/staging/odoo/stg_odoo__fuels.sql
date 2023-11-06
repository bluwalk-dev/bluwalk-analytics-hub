select
    *,
    SHA256(CONCAT(
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',CAST(start_date AS TIMESTAMP)),
        card_name,
        CAST(quantity AS NUMERIC))) AS surrogateKey
from {{ source('odoo_realtime', 'fuel') }}