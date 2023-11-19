with

source as (
    select
        *
    from {{ source('generic', 'quarter_params') }}
),

transformation as (

    select
        
        pipeline_id,
        marketplace partner_marketplace,
        partner_name,
        activation_point_score,
        activation_team,
        marketing_point_score,
        conversion_rate,
        year_quarter,
        churn_rate,
        lifespan,
        monthly_revenue_per_user,
        lifetime_value

    from source

)

select * from transformation