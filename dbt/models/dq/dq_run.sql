{{ config(indexes=[{'columns': ['dataset', 'run_id', 'round'], 'unique': true}]) }}

-- One row per run and round: record counts and DQ score. Counts after
-- round 1 are as of the end of that round (running totals over the rounds),
-- so the latest round of a run reads e.g. 350 issues -> 200 auto-fixed ->
-- 130 manually fixed -> 20 unresolved. Definitions: _dq_models.yml.

with rounds as (

    select * from {{ ref('stg_dq__rounds') }}

),

first_round as (

    select
        dataset,
        run_id,
        result ->> 'source_file' as source_file,
        (result -> 'ingestion' ->> 'ingested_at')::timestamptz as ingested_at,
        result -> 'config' ->> 'schema_version' as schema_version,
        result -> 'config' ->> 'rules_version' as rules_version,
        (counts ->> 'input_rows')::bigint as input_rows,
        (counts ->> 'duplicate_rows_removed')::bigint as duplicate_rows_removed,
        (counts ->> 'processed_rows')::bigint as processed,
        (counts ->> 'accepted_rows')::bigint as valid,
        (counts ->> 'accepted_with_warnings_rows')::bigint as valid_with_warnings,
        (counts ->> 'quarantined_rows')::bigint as quarantined
    from rounds
    where round = 1

),

-- Valid records that needed at least one automatic repair: repaired in
-- round 1 and not quarantined (every quarantined record is in the ledger).
auto_fixed as (

    select audit.dataset, audit.run_id, count(distinct audit.record_id) as auto_fixed_records
    from {{ source('dq', 'remediation_audit') }} as audit
    where audit.round = 1
      and audit.outcome in ('remediated', 'nullified_in_accepted')
      and not exists (
          select 1
          from {{ source('dq', 'quarantine_status') }} as ledger
          where ledger.dataset = audit.dataset
            and ledger.run_id = audit.run_id
            and ledger.record_id = audit.record_id
      )
    group by audit.dataset, audit.run_id

),

-- Manual action behind each corrected record of a round (a FIX wins over
-- an ACCEPT of the same record).
actions as (

    select
        dataset,
        run_id,
        round,
        record_id,
        bool_or(action = 'FIX') as fixed,
        bool_or(action = 'ACCEPT') as accepted
    from {{ source('dq', 'correction') }}
    where action in ('FIX', 'ACCEPT')
    group by dataset, run_id, round, record_id

),

-- Records that passed re-validation of round k: RESOLVED with round k in
-- the ledger history.
resolved as (

    select distinct dataset, run_id, round as revalidated_round, record_id
    from {{ source('dq', 'quarantine_history') }}
    where status = 'RESOLVED'

),

resolved_by_action as (

    select
        rounds.dataset,
        rounds.run_id,
        rounds.round,
        count(*) filter (where actions.fixed) as manually_fixed,
        count(*) filter (where actions.accepted and not actions.fixed) as confirmed
    from rounds
    join resolved
      on resolved.dataset = rounds.dataset
     and resolved.run_id = rounds.run_id
     and resolved.revalidated_round = rounds.revalidated_round
    join actions
      on actions.dataset = resolved.dataset
     and actions.run_id = resolved.run_id
     and actions.round = resolved.revalidated_round
     and actions.record_id = resolved.record_id
    where rounds.round > 1
    group by rounds.dataset, rounds.run_id, rounds.round

),

-- What each revalidation round did (null for round 1).
round_counts as (

    select
        rounds.dataset,
        rounds.run_id,
        rounds.round,
        rounds.round_type,
        rounds.revalidated_round,
        rounds.started_at,
        rounds.finished_at,
        case when rounds.round > 1 then (rounds.counts ->> 'open_before')::bigint end
            as round_open_before,
        case when rounds.round > 1 then (rounds.counts ->> 'revalidated')::bigint end
            as round_revalidated,
        case when rounds.round > 1 then (rounds.counts ->> 'resolved')::bigint end
            as round_resolved,
        case when rounds.round > 1 then (rounds.counts ->> 'failed_again')::bigint end
            as round_failed_again,
        case when rounds.round > 1 then (rounds.counts ->> 'manual_review_required')::bigint end
            as round_manual_review,
        case when rounds.round > 1
            then (rounds.counts ->> 'discarded')::bigint
                 + (rounds.counts ->> 'removed_as_duplicate')::bigint
        end as round_discarded,
        case when rounds.round > 1 then (rounds.counts ->> 'open_after')::bigint end
            as round_open_after,
        coalesce(resolved_by_action.manually_fixed, 0) as round_manually_fixed,
        coalesce(resolved_by_action.confirmed, 0) as round_confirmed
    from rounds
    left join resolved_by_action using (dataset, run_id, round)

),

running as (

    select
        round_counts.*,
        sum(coalesce(round_resolved, 0)) over run_so_far as resolved,
        sum(round_manually_fixed) over run_so_far as manually_fixed_records,
        sum(round_confirmed) over run_so_far as confirmed_records,
        sum(coalesce(round_discarded, 0)) over run_so_far as discarded,
        sum(coalesce(round_manual_review, 0)) over run_so_far as manual_review,
        round = max(round) over (partition by dataset, run_id) as is_latest_round
    from round_counts
    window run_so_far as (
        partition by dataset, run_id order by round
        rows between unbounded preceding and current row
    )

)

select
    running.run_id,
    running.dataset,
    first_round.source_file,
    first_round.ingested_at,
    first_round.schema_version,
    first_round.rules_version,
    first_round.input_rows,
    first_round.duplicate_rows_removed,
    first_round.processed,
    first_round.valid,
    first_round.valid_with_warnings,
    coalesce(auto_fixed.auto_fixed_records, 0)::bigint as auto_fixed_records,
    first_round.quarantined,
    (coalesce(auto_fixed.auto_fixed_records, 0) + first_round.quarantined)::bigint
        as records_with_issues,
    {{ score('first_round.valid', 'first_round.processed') }} as initial_dq_score,
    running.round,
    running.round_type,
    running.revalidated_round,
    running.started_at,
    running.finished_at,
    running.round_open_before,
    running.round_revalidated,
    running.round_resolved,
    running.round_failed_again,
    running.round_manual_review,
    running.round_discarded,
    running.resolved::bigint as resolved,
    running.manually_fixed_records::bigint as manually_fixed_records,
    running.confirmed_records::bigint as confirmed_records,
    (running.resolved - running.manually_fixed_records - running.confirmed_records)::bigint
        as auto_resolved_records,
    running.discarded::bigint as discarded,
    running.manual_review::bigint as manual_review,
    -- Open: everything quarantined after round 1, then what the latest
    -- re-validation left open.
    coalesce(running.round_open_after, first_round.quarantined) as open,
    (coalesce(running.round_open_after, first_round.quarantined) + running.manual_review)::bigint
        as unresolved_records,
    {{ score('first_round.valid + running.resolved', 'first_round.processed') }}
        as final_dq_score,
    running.is_latest_round
from running
join first_round using (dataset, run_id)
left join auto_fixed using (dataset, run_id)
