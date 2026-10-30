-- Every automatic repair (remediation audit) and manual correction
-- (correction log), with its outcome.

WITH rounds AS (
    SELECT dataset, run_id, round, revalidated_round, finished_at
    FROM {{ ref('dq_validated_round') }}
),

runs AS (
    SELECT DISTINCT dataset, run_id FROM rounds
),

automatic AS (
    SELECT
        audit.dataset,
        audit.run_id,
        audit.round,
        audit.record_id,
        audit."column",
        'automatic' AS correction_type,
        audit.action,
        audit.rule_id,
        audit.original_value AS old_value,
        audit.cleaned_value AS new_value,
        audit.outcome,
        NULL::text AS reason,
        'dq_engine' AS "user",
        rounds.finished_at AS corrected_at
    FROM {{ source('dq', 'remediation_audit') }} AS audit
    JOIN runs USING (dataset, run_id)
    LEFT JOIN rounds USING (dataset, run_id, round)
),

-- A corrected record's status right after its round was re-validated: the
-- ledger change stamped with that re-validation's finish time.
status_after AS (
    SELECT DISTINCT ON (history.dataset, history.run_id, history.record_id, rounds.revalidated_round)
        history.dataset,
        history.run_id,
        history.record_id,
        rounds.revalidated_round AS round,
        history.status
    FROM {{ source('dq', 'quarantine_history') }} AS history
    JOIN rounds
      ON rounds.dataset = history.dataset
     AND rounds.run_id = history.run_id
     AND rounds.finished_at = history.changed_at
    WHERE rounds.revalidated_round IS NOT NULL
    ORDER BY history.dataset, history.run_id, history.record_id, rounds.revalidated_round,
             history.history_id DESC
),

manual AS (
    SELECT
        log.dataset,
        log.run_id,
        log.round,
        log.record_id,
        log."column",
        'manual' AS correction_type,
        log.action,
        NULL::text AS rule_id,
        log.old_value,
        log.new_value,
        -- A field corrected again in the same round: only the newest line
        -- was applied.
        CASE
            WHEN row_number() OVER (
                PARTITION BY log.dataset, log.run_id, log.round, log.record_id,
                             log."column", log.action
                ORDER BY log.log_id DESC
            ) > 1 THEN 'superseded'
            WHEN status_after.status = 'RESOLVED' THEN 'resolved'
            WHEN status_after.status = 'OPEN' THEN 'failed_revalidation'
            WHEN status_after.status = 'MANUAL_REVIEW_REQUIRED' THEN 'manual_review_required'
            WHEN status_after.status = 'DISCARDED' THEN 'discarded'
            ELSE 'pending'
        END AS outcome,
        log.reason,
        log."user",
        log.corrected_at
    FROM {{ source('dq', 'correction_log') }} AS log
    JOIN runs USING (dataset, run_id)
    LEFT JOIN status_after USING (dataset, run_id, record_id, round)
)

SELECT
    run_id, dataset, round, record_id, "column", correction_type, action, rule_id,
    old_value, new_value, outcome, reason, "user", corrected_at
FROM automatic

UNION ALL

SELECT
    run_id, dataset, round, record_id, "column", correction_type, action, rule_id,
    old_value, new_value, outcome, reason, "user", corrected_at
FROM manual
