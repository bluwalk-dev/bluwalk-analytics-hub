with

source as (
    SELECT
        * 
    FROM {{ source('mds', 'reconciliation') }}
),

transformation as (

    select
        CAST(month AS INT) as year_month,
        cliente,
        segurador,
        ramo,
        apolice,
        data_inicio_da_apolice,
        fraccionamento,
        id_externo,
        numero_do_recibo,
        data_de_cobranca,
        data_inicio,
        data_fim,
        matricula,
        tipo_de_recibo,
        informacao_adicional,
        premio_comercial,
        premio_total,
        comissao_total,
        angariacao_base,
        angariacao_valor_base,
        angariacao_percentagem,
        angariacao,
        cobranca_base,
        cobranca_valor_base,
        cobranca_percentagem,
        cobranca,
        corretagem_base,
        corretagem_valor_base,
        corretagem_percentagem,
        corretagem,
        extra_base,
        extra_valor_base,
        extra_percentagem,
        extra,
        fixa_base,
        fixa_valor_base,
        percentagem_fixa,
        fixa,
        imposto_selo,
        comissao_total_cedida,
        valor_sem_iva,
        iva,
        valor_a_pagar,
        load_timestamp


    from source
)

select * from transformation
