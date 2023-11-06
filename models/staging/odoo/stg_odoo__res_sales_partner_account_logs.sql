{{ config(
    materialized='view'
) }}

select
    *
from {{ source('odoo_realtime', 'res_sales_partner_account_log') }}