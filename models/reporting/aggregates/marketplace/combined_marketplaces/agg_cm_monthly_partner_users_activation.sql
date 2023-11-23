-- Selecting aggregated activation data by year_month
SELECT
    year_month, -- Grouping identifier for the period (year and month)
    -- Summing the number of new activations for each year_month
    partner_key,
    partner_name,
    partner_marketplace,
    partner_category,
    sum(IF(activation_type='new', nr_activations, 0)) as new_activations,
    -- Summing the number of reactivations for each year_month
    sum(IF(activation_type='reactivation', nr_activations, 0)) as re_activations,
    -- Summing the total number of activations regardless of type for each year_month
    sum(nr_activations) as nr_activations
FROM (
    -- Subquery to count activations by year_month and activation type
    SELECT 
        year_month, -- The year and month of the activation
        partner_key,
        partner_name,
        partner_marketplace,
        partner_category,
        activation_type, -- The type of activation: 'new' or 'reactivation'
        count(*) as nr_activations -- Counting the number of activations
    FROM {{ ref('int_activation_monthly_partner_activations_list') }} -- Referencing a DBT model for monthly user activations
    GROUP BY  -- Grouping the data by year_month and activation type to prepare for aggregation
        year_month, 
        activation_type,
        partner_key,
        partner_name,
        partner_marketplace,
        partner_category

    -- The ORDER BY in this subquery will not affect the final result because aggregation in the outer query does not preserve order
    ORDER BY year_month, activation_type DESC
) x
-- Grouping the outer query by year_month to get one row per month
GROUP BY 
    year_month,
    partner_name,
    partner_marketplace,
    partner_category,
    partner_key
-- Ordering the final result set by year_month in descending order to show the latest periods first
ORDER BY year_month DESC