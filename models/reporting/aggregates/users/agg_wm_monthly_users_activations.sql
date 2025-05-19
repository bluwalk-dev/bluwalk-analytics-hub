SELECT
    year_month,
    sum(IF(activation_type='new', nr_activations, 0)) as new_activations,
    sum(IF(activation_type='reactivation', nr_activations, 0)) as re_activations,
    sum(nr_activations) as nr_activations
FROM (
    -- Subquery to count activations by year_month and activation type
    SELECT 
        year_month, -- The year and month of the activation
        activation_type, -- The type of activation: 'new' or 'reactivation'
        count(*) as nr_activations -- Counting the number of activations
    FROM {{ ref('int_user_monthly_activations_list') }} -- Referencing a DBT model for monthly user activations
    GROUP BY year_month, activation_type -- Grouping the data by year_month and activation type to prepare for aggregation
    -- The ORDER BY in this subquery will not affect the final result because aggregation in the outer query does not preserve order
    ORDER BY year_month, activation_type DESC
) x

GROUP BY year_month
ORDER BY year_month DESC