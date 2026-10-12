{{ config(indexes=[{'columns': ['dataset', 'run_id', 'round']}]) }}

-- Automatic repairs (remediation audit) and manual corrections (correction
-- log), with their outcome.

with rounds as (

    select dataset, run_id, round, revalidated_round, finished_at
    from {{ ref('stg_dq__rounds') }}

),

automatic as (

    select
        audit.dataset,
        audit.run_id,
        audit.round,
        audit.record_id,
        audit."column",
        'automatic' as correction_type,
        audit.action,
        audit.rule_id,
        audit.original_value as old_value,
        audit.cleaned_value as new_value,
        audit.outcome,
        null::text as reason,
        'dq_engine' as "user",
        rounds.finished_at as corrected_at
    from {{ source('dq', 'remediation_audit') }} as audit
    join {{ ref('stg_dq__runs') }} using (dataset, run_id)
    left join rounds using (dataset, run_id, round)

),

-- A corrected record's status right after its round was re-validated: the
-- ledger change stamped with that re-validation's finish time.
status_after as (

    select distinct on (history.dataset, history.run_id, history.record_id, rounds.revalidated_round)
        history.dataset,
        history.run_id,
        history.record_id,
        rounds.revalidated_round as round,
        history.status
    from {{ source('dq', 'quarantine_history') }} as history
    join rounds
      on rounds.dataset = history.dataset
     and rounds.run_id = history.run_id
     and rounds.finished_at = history.changed_at
    where rounds.revalidated_round is not null
    order by history.dataset, history.run_id, history.record_id, rounds.revalidated_round,
             history.history_id desc

),

manual as (

    select
        log.dataset,
        log.run_id,
        log.round,
        log.record_id,
        log."column",
        'manual' as correction_type,
        log.action,
        null::text as rule_id,
        log.old_value,
        log.new_value,
        -- A field corrected again in the same round: only the newest line
        -- was applied.
        case
            when row_number() over (
                partition by log.dataset, log.run_id, log.round, log.record_id,
                             log."column", log.action
                order by log.log_id desc
            ) > 1 then 'superseded'
            when status_after.status = 'RESOLVED' then 'resolved'
            when status_after.status = 'OPEN' then 'failed_revalidation'
            when status_after.status = 'MANUAL_REVIEW_REQUIRED' then 'manual_review_required'
            when status_after.status = 'DISCARDED' then 'discarded'
            else 'pending'
        end as outcome,
        log.reason,
        log."user",
        log.corrected_at
    from {{ source('dq', 'correction_log') }} as log
    join {{ ref('stg_dq__runs') }} using (dataset, run_id)
    left join status_after using (dataset, run_id, record_id, round)

)

select
    run_id, dataset, round, record_id, "column", correction_type, action, rule_id,
    old_value, new_value, outcome, reason, "user", corrected_at
from automatic

union all

select
    run_id, dataset, round, record_id, "column", correction_type, action, rule_id,
    old_value, new_value, outcome, reason, "user", corrected_at
from manual
