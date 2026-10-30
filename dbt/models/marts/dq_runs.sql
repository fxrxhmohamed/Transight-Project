-- The validated runs as MetricFlow reads them: one row per run, with its
-- current figures (the latest round of dq_run: counts after every
-- correction round so far).
--
-- run_date is the day the run's file was ingested: the time the DQ metrics
-- are grouped by (month, year).

select
    run_id,
    dataset,
    source_file,
    coalesce(ingested_at, finished_at)::date as run_date,
    schema_version,
    rules_version,
    round                                   as rounds,
    processed,
    valid,
    valid_with_warnings,
    auto_fixed_records,
    quarantined,
    records_with_issues,
    resolved,
    valid + resolved                        as clean_records,
    manually_fixed_records,
    confirmed_records,
    discarded,
    unresolved_records
from {{ ref('dq_run') }}
where is_latest_round
