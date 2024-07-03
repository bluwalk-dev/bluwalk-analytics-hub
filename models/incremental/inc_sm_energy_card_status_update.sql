WITH bp_cards AS (
    SELECT
        id AS card_id, 
        name AS card_name, 
        status AS card_status
    FROM {{ ref('stg_odoo__fuel_cards') }}
    WHERE service_partner_id = 1
)
SELECT
    card_id,
    new_card_status
FROM (
    SELECT
        a.card_id,
        a.card_name,
        a.card_status,
        LOWER(CASE
            WHEN b.card_status = 'Activo' THEN
                CASE
                    WHEN b.profile = 'Incumprimento' THEN 'disabled'
                    ELSE 'enabled'
                END
            ELSE 'destroyed'
        END) AS new_card_status
    FROM bp_cards a
    LEFT JOIN {{ ref('base_bp_card_status') }} b ON a.card_name = b.card_name
    WHERE b.card_name IS NOT NULL
) subquery
WHERE new_card_status != COALESCE(card_status, '')