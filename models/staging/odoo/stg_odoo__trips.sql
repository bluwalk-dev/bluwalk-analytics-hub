with

source as (
    select
        *
    from {{ source('google_cloud_postgresql_public', 'trips') }}
),

transformation as (

    select
        id job_order_id,
        name job_order_name,
        status job_order_status,
        payment_cycle statement,
        billing_account_id,
        partner_id contact_id,
        sales_partner_id,
        sales_partner_driver user_partner_uuid,
        sales_account_city location,
        sales_segment stream,
        period_start start_date,
        period_end end_date,
        gross_sales sales_gross,
        net_sales sales_net,
        sales_taxes,
        sales_tax_rate,
        gross_partner_fee partner_fee_gross,
        net_partner_fee partner_fee_net,
        partner_fee_taxes,
        partner_fee_tax_rate,
        partner_payment partner_payout,
        create_uid,
        create_date,
        write_uid,
        write_date,
        res_sales_partner_id,
        nr_trips

    from source

)

select * from transformation
where _fivetran_deleted IS FALSE