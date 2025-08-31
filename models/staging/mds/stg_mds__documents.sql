with

source as (
    SELECT
        * 
    FROM `bluwalk-analytics-hub.source_mds.src_mds_documents`
),

transformation as (

    select
        
        CONCAT(NUM_APOLICE, ':', NUM_RECIBO) as document_id,
        CASE
            WHEN NUM_APOLICE = '206344122' THEN 'Drivfit'
            ELSE 'Bluwalk'
        END scope,
        NUM_APOLICE as policy_nr,
        NUM_RECIBO as document_nr,
        TIPO_DOCUMENTO as document_type,
        SAFE.PARSE_DATE('%Y-%m-%d', DATA_INICIO) as start_date,
        SAFE.PARSE_DATE('%Y-%m-%d', DATA_FIM) as end_date,
        SAFE.PARSE_DATE('%Y-%m-%d', DATA_VENCIMENTO) as due_date,
        ESTADO as status,
        SAFE.PARSE_DATE('%Y-%m-%d', DATA_SITUACAO) as last_update,
        CAST(PREMIO_TOTAL as NUMERIC) as premium_total,
        CAST(PREMIO_COMERCIAL as NUMERIC) as premium_comercial,
        CAST(COMISSAO_MDS as NUMERIC) as commission_mds,
        CAST(COMISSAO_PARCEIRO as NUMERIC) as commission_partner,
        GESTAO_DE_COBRANCA as collection_management,
        REPLACE(MODO_PAGAMENTO, 'nan', '') as payment_method,
        load_timestamp,
        run_id

    from source
)

select * from transformation
order by last_update desc
