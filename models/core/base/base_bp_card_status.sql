WITH unique_transactions AS (
    SELECT * FROM (
        SELECT
            *, ROW_NUMBER() OVER (
                PARTITION BY card 
                ORDER BY load_timestamp DESC
            ) AS __row_number
        FROM {{ ref("stg_bp__cards") }}
    )
    WHERE __row_number = 1
)

SELECT
    card,
    CONCAT('B',LPAD(CAST(card AS STRING), 6, '0')) card_name,
    additional_information,
    card_holder,
    validity,
    service_code,
    card_scope,
    card_status,
    profile,
    km_reading,
    last_card_use
FROM unique_transactions