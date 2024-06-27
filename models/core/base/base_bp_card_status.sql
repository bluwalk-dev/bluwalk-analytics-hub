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
    *
FROM unique_transactions