SELECT
  *
FROM {{ ref("base_mds_documents") }}
WHERE 
    start_date >= '2023-01-01' AND
    policy_nr NOT IN (
        SELECT policy_nr FROM {{ ref("inc_sm_insurance_policy_missing") }}
    ) AND
    scope = 'Bluwalk' AND
    status NOT IN ('Anulado', 'Em Pagamento')
ORDER BY last_update DESC
