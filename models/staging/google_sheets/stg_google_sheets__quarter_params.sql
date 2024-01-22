{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('google_sheets', 'quarter_params') }}
),

transformation as (

    select
        
        CAST(year_quarter AS STRING) year_quarter,
        CAST(partner_key AS STRING) partner_key,
        CAST(pipeline_id AS STRING) pipeline_id,
        CAST(marketplace AS STRING) partner_marketplace,
        CAST(partner_name AS STRING) partner_name,
        CAST(activation_point_score AS INTEGER) activation_point_score,
        CAST(activation_team AS STRING) activation_team,
        CAST(marketing_point_score AS INTEGER) marketing_point_score,
        CAST(conversion_rate AS NUMERIC) conversion_rate,
        CAST(churn_rate AS NUMERIC) churn_rate,
        CAST(lifespan AS NUMERIC) lifespan,
        CAST(monthly_revenue_per_user AS NUMERIC) monthly_revenue_per_user,
        CAST(lifetime_value AS NUMERIC) lifetime_value

    FROM source
    WHERE year_quarter IS NOT NULL

)

select * from transformation