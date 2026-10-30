-- One row per run, round and rule: checked, failed, repaired and
-- unresolved values. Re-validation rounds count the corrected records only.

SELECT
    rounds.run_id,
    rounds.dataset,
    rounds.round,
    rounds.round_type,
    rounds.finished_at,
    rule ->> 'rule_id' AS rule_id,
    rule ->> 'name' AS rule_name,
    rule ->> 'type' AS rule_type,
    rule ->> 'column' AS "column",
    rule ->> 'dimension' AS dimension,
    rule ->> 'severity' AS severity,
    rule ->> 'error_code' AS error_code,
    rule ->> 'error_category' AS error_category,
    rule ->> 'origin' AS origin,
    (rule ->> 'checked')::bigint AS checked,
    (rule ->> 'detected')::bigint AS failed,
    (rule ->> 'remediated')::bigint AS repaired,
    (rule ->> 'unresolved')::bigint AS unresolved
FROM {{ ref('dq_validated_round') }} AS rounds
CROSS JOIN LATERAL jsonb_array_elements(coalesce(rounds.result -> 'rules', '[]'::jsonb)) AS rule
