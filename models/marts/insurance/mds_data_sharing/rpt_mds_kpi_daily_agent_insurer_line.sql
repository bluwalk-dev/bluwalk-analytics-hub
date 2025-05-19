WITH
all_combinations AS (
    SELECT
        c.date,
        i.insurer_contact_id,
        i.insurer_name,
        it.insurance_type_id,
        it.insurance_type,
        o.hs_owner_id as owner_id,
        o.owner_name
    FROM (
        SELECT DISTINCT insurer_contact_id, insurer_name
        FROM {{ ref("dim_insurance_policies") }}
    ) i
    CROSS JOIN {{ ref("dim_insurance_types") }} it
    CROSS JOIN {{ ref("util_calendar") }} c
    CROSS JOIN (
        SELECT DISTINCT hs_owner_id, owner_name
        FROM {{ ref("fct_deals") }}
    ) o
),
policies AS (
    SELECT
        c.close_date,
        b.insurer_contact_id,
        b.insurance_type_id,
        c.hs_owner_id as owner_id,
        COUNT(*) as policies_issued
    FROM {{ ref("dim_insurance_policies")}} b
    LEFT JOIN {{ ref("fct_deals")}} c ON b.insurance_policy_name = c.insurance_policy_name
    WHERE 
        c.deal_pipeline_id = 'default' AND 
        hs_owner_id IS NOT NULL
    GROUP BY
        close_date,
        insurer_contact_id,
        insurance_type_id,
        owner_id
),
policies_accepted AS (
    SELECT
        DATE(insurance_entered_accepted) date,
        insurance_insurer_id as insurer_contact_id,
        insurance_policy_type_id as insurance_type_id,
        hs_owner_id as owner_id,
        COUNT(*) as policies_accepted,
        SUM(insurance_annual_premium) as policies_premium
    FROM {{ ref("fct_deals") }}
    WHERE 
        deal_pipeline_id = 'default' AND 
        insurance_entered_accepted IS NOT NULL AND 
        hs_owner_id IS NOT NULL
    GROUP BY
        date,
        insurance_insurer_id,
        insurance_policy_type_id,
        hs_owner_id
)

SELECT
    a.date,
    a.insurer_name,
    a.insurance_type,
    a.owner_name,
    COALESCE(b.policies_issued, 0) policies_issued,
    COALESCE(c.policies_accepted, 0) policies_accepted,
    COALESCE(c.policies_premium, 0) policies_premium
FROM all_combinations a
LEFT JOIN policies b ON 
    a.date = b.close_date AND 
    a.insurer_contact_id = b.insurer_contact_id AND
    a.insurance_type_id = b.insurance_type_id AND
    a.owner_id = b.owner_id
LEFT JOIN policies_accepted c ON
    a.date = c.date AND 
    a.insurer_contact_id = c.insurer_contact_id AND
    a.insurance_type_id = c.insurance_type_id AND
    a.owner_id = c.owner_id
WHERE 
    a.date BETWEEN '2023-01-01' AND CURRENT_DATE() AND
    (COALESCE(b.policies_issued, 0) > 0 OR COALESCE(c.policies_accepted, 0) > 0)
ORDER BY date DESC