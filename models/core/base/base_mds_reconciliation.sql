SELECT 
    year_month,
    cliente,
    segurador,
    ramo,
    apolice,
    data_inicio_da_apolice,
    fraccionamento,
    numero_do_recibo,
    data_de_cobranca,
    data_inicio,
    data_fim,
    tipo_de_recibo,
    premio_total,
    valor_a_pagar
FROM
    (SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY year_month, numero_do_recibo
            ORDER BY load_timestamp DESC
        ) AS __row_number
    FROM {{ ref("stg_mds__reconciliation") }} 
    )
WHERE __row_number = 1