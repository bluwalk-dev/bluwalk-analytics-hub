WITH fuel_card_log AS (
    SELECT * FROM (
        SELECT
            *, ROW_NUMBER() OVER (
                PARTITION BY name 
                ORDER BY delivery_date DESC
            ) AS __row_number
        FROM {{ ref("stg_odoo__fuel_card_log") }}
    )
    WHERE __row_number = 1
)

SELECT
    name as card_name,
    supplier_id,
    partner_id as contact_id,
    delivery_date,
    receive_date,
    b.validity,
    b.card_status,
    b.profile,
    b.last_card_use
FROM fuel_card_log a
LEFT JOIN {{ ref("base_bp_card_status") }} b ON a.name = b.card_name