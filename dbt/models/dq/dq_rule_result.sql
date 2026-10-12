{{ config(indexes=[{'columns': ['dataset', 'run_id', 'round']}]) }}

-- One row per run, round and rule: the rule statistics of each round's
-- summary (re-validation rounds count the corrected records only).

select
    rounds.run_id,
    rounds.dataset,
    rounds.round,
    rounds.round_type,
    rounds.finished_at,
    rule ->> 'rule_id' as rule_id,
    rule ->> 'name' as rule_name,
    rule ->> 'type' as rule_type,
    rule ->> 'column' as "column",
    rule ->> 'dimension' as dimension,
    rule ->> 'severity' as severity,
    rule ->> 'error_code' as error_code,
    rule ->> 'error_category' as error_category,
    rule ->> 'origin' as origin,
    (rule ->> 'checked')::bigint as checked,
    (rule ->> 'detected')::bigint as failed,
    (rule ->> 'remediated')::bigint as repaired,
    (rule ->> 'unresolved')::bigint as unresolved
from {{ ref('stg_dq__rounds') }} as rounds
cross join lateral jsonb_array_elements(coalesce(rounds.result -> 'rules', '[]'::jsonb)) as rule
