SELECT
  *
FROM 
    (SELECT scope, status, policy_nr, MIN(start_date) start_date, MIN(last_update) last_update
    FROM {{ ref("base_mds_documents") }}
    GROUP BY scope, status, policy_nr) a
LEFT JOIN {{ ref("dim_insurance_policies") }} b ON a.policy_nr = b.insurance_policy_number
WHERE
    start_date >= '2023-01-01' AND
    insurance_policy_id IS NULL AND
    scope = 'Bluwalk' AND
    status NOT IN ('Anulado', 'Em Pagamento')
ORDER BY last_update DESC