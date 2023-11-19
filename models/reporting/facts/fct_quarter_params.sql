select
    year_quarter,
    pipeline_id hs_pipeline_id,
    '' partner_key,
    partner_marketplace,
    partner_name,
    activation_point_score,
    marketing_point_score,
    activation_team,
    conversion_rate,
    churn_rate,
    lifespan,
    monthly_revenue_per_user,
    lifetime_value
from {{ ref('stg_generic__quarter_params') }}