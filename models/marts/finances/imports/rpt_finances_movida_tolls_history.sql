SELECT
  license_plate,
  exit_date
FROM {{ ref('stg_odoo_drivfit__tolls') }}
WHERE obe_code is null