WITH last_record AS (
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
    name,
    supplier_id,
    partner_id,
    delivery_date,
    receive_date,
    b.validity,
    b.card_status,
    b.profile,
    b.last_card_use
FROM last_record a
LEFT JOIN {{ ref("base_bp_card_status") }} b ON a.name = b.card_name
WHERE supplier_id = 2943 AND validity IS NOT NULL