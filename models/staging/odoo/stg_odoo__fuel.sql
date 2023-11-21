with

source as (
    select
        *
    from {{ source('google_cloud_postgresql_public', 'fuel') }}
),

transformation as (

    select
        
        TO_HEX(MD5(end_date_ts || card_name || quantity)) as transaction_key,
        *
    FROM (
        select
            id,
            name,
            card_name,
            CAST(start_date AS TIMESTAMP) start_date_ts,
            start_date,
            CAST(end_date AS TIMESTAMP) end_date_ts,
            end_date,
            station_name,
            station_type,
            fuel_source,
            partner_id,
            product,
            ROUND(quantity,2) quantity,
            measurement_unit,
            ROUND(cost,2) cost,
            ROUND(margin,2) margin,
            ROUND((cost+margin),2) value_station_price,
            payment_cycle,
            status,
            supplier_id,
            create_date,
            create_uid,
            write_date,
            write_uid,
            ba_credit,
            ba_credit_product_id,
            ba_debit,
            ba_debit_product_id,
            billing_account_id,
            company_id

        from source
        where _fivetran_deleted IS FALSE
    )
)

select * from transformation