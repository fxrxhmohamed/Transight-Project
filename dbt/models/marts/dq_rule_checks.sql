-- The rules as MetricFlow reads them: one row per run and rule of the
-- first validation (round 1), which checks every record; re-validation
-- rounds check the corrected records only and are left out. run_date is
-- the run's (dq_runs).

select
    rules.dataset || '|' || rules.run_id || '|' || rules.rule_id as rule_check_id,
    rules.run_id,
    rules.dataset,
    runs.run_date,
    rules.rule_id,
    rules.rule_name,
    rules.rule_type,
    rules."column"                          as column_name,
    rules.dimension                         as dq_dimension,
    rules.severity,
    rules.error_code,
    rules.origin,
    coalesce(rules.checked, 0)              as checked,
    coalesce(rules.failed, 0)               as failed,
    coalesce(rules.repaired, 0)             as repaired,
    coalesce(rules.unresolved, 0)           as unresolved
from {{ ref('dq_rule_result') }} as rules
join {{ ref('dq_runs') }} as runs
  on runs.run_id = rules.run_id
 and runs.dataset = rules.dataset
where rules.round = 1
