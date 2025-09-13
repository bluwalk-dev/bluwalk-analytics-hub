WITH 
insurance_quotes AS (
    SELECT 
        deal_id,
        owner_name as agent_name,
        insurance_entered_proposal_sent AS sent_date,
        insurer
    FROM 
        bluwalk-analytics-hub.core.core_hubspot_deals,
        UNNEST([
            STRUCT('Allianz' AS insurer, insurance_quote_allianz AS quote_status),
            STRUCT('Tranquilidade' AS insurer, insurance_quote_tranquilidade AS quote_status),
            STRUCT('Fidelidade' AS insurer, insurance_quote_fidelidade AS quote_status),
            STRUCT('Lusitania' AS insurer, insurance_quote_lusitania AS quote_status)
        ]) AS quotes
    WHERE quote_status = TRUE AND insurance_entered_proposal_sent IS NOT NULL
)

SELECT
    DATE(sent_date) as date,
    agent_name,
    insurer,
    count(*) as quotes_sent
FROM insurance_quotes
GROUP BY date, insurer, agent_name
ORDER BY date DESC