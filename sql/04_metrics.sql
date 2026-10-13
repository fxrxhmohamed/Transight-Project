-- DQ metric views for Power BI, in the metrics schema, over the dq schema
-- (sql/02_dq.sql):
--
--   metrics.dq_run          one row per run and round: record counts and
--                           DQ scores
--   metrics.dq_rule_result  one row per run, round and rule: checked,
--                           failed, repaired, unresolved
--   metrics.dq_quarantine   every status change of every quarantined record
--   metrics.dq_correction   every automatic repair and manual correction,
--                           with its outcome
--
-- Views, so they always show the dq schema as it is; Power BI imports them
-- on refresh. They cover every validated run (a run with a round 1 result).
-- dq.validated_round is their shared input: every round with the fields of
-- its JSON summary.
--
-- Docker runs this file once, when the database volume is first created;
-- "python dq.py metrics" applies it again (and checks the views), so a
-- change here reaches the database on the next metrics step.
-- Manually: psql -U metaflow -d metaflow -f sql/04_metrics.sql (safe to run again).

CREATE SCHEMA IF NOT EXISTS metrics;

-- Older databases hold these as tables (built by dbt): replace them.
DO $$
DECLARE
    name text;
BEGIN
    FOREACH name IN ARRAY ARRAY['dq_run', 'dq_rule_result', 'dq_quarantine', 'dq_correction'] LOOP
        IF to_regclass('metrics.' || name) IS NOT NULL
           AND (SELECT relkind FROM pg_class WHERE oid = to_regclass('metrics.' || name)) = 'r' THEN
            EXECUTE format('DROP TABLE metrics.%I CASCADE', name);
        END IF;
    END LOOP;
END $$;

-- Dropped and created again: CREATE OR REPLACE VIEW cannot change columns.
DROP VIEW IF EXISTS dq.validated_round CASCADE;

-- ---- Rounds -------------------------------------------------------------

CREATE VIEW dq.validated_round AS
WITH validated AS (
    SELECT dataset, run_id
    FROM dq.round_result
    WHERE round = 1
)
SELECT
    results.dataset,
    results.run_id,
    results.round,
    CASE WHEN results.round = 1 THEN 'validation' ELSE 'revalidation' END AS round_type,
    (results.result ->> 'revalidated_round')::integer AS revalidated_round,
    (results.result ->> 'started_at')::timestamptz AS started_at,
    (results.result ->> 'finished_at')::timestamptz AS finished_at,
    results.result -> 'counts' AS counts,
    results.result
FROM dq.round_result AS results
JOIN validated USING (dataset, run_id);

COMMENT ON VIEW dq.validated_round IS
    'Every round of every validated run, with the fields of its JSON summary (input of the metrics views).';

-- ---- dq_run -------------------------------------------------------------
-- Counts after round 1 are as of the end of that round (running totals over
-- the rounds), so the latest round of a run reads e.g. 350 issues -> 200
-- auto-fixed -> 130 manually fixed -> 20 unresolved.

CREATE VIEW metrics.dq_run AS
WITH rounds AS (
    SELECT * FROM dq.validated_round
),

first_round AS (
    SELECT
        dataset,
        run_id,
        result ->> 'source_file' AS source_file,
        (result -> 'ingestion' ->> 'ingested_at')::timestamptz AS ingested_at,
        result -> 'config' ->> 'schema_version' AS schema_version,
        result -> 'config' ->> 'rules_version' AS rules_version,
        (counts ->> 'input_rows')::bigint AS input_rows,
        (counts ->> 'duplicate_rows_removed')::bigint AS duplicate_rows_removed,
        (counts ->> 'processed_rows')::bigint AS processed,
        (counts ->> 'accepted_rows')::bigint AS valid,
        (counts ->> 'accepted_with_warnings_rows')::bigint AS valid_with_warnings,
        (counts ->> 'quarantined_rows')::bigint AS quarantined
    FROM rounds
    WHERE round = 1
),

-- Valid records that needed at least one automatic repair: repaired in
-- round 1 and not quarantined (every quarantined record is in the ledger).
auto_fixed AS (
    SELECT audit.dataset, audit.run_id, count(DISTINCT audit.record_id) AS auto_fixed_records
    FROM dq.remediation_audit AS audit
    WHERE audit.round = 1
      AND audit.outcome IN ('remediated', 'nullified_in_accepted')
      AND NOT EXISTS (
          SELECT 1
          FROM dq.quarantine_status AS ledger
          WHERE ledger.dataset = audit.dataset
            AND ledger.run_id = audit.run_id
            AND ledger.record_id = audit.record_id
      )
    GROUP BY audit.dataset, audit.run_id
),

-- Manual action behind each corrected record of a round (a FIX wins over
-- an ACCEPT of the same record).
actions AS (
    SELECT
        dataset,
        run_id,
        round,
        record_id,
        bool_or(action = 'FIX') AS fixed,
        bool_or(action = 'ACCEPT') AS accepted
    FROM dq.correction
    WHERE action IN ('FIX', 'ACCEPT')
    GROUP BY dataset, run_id, round, record_id
),

-- Records that passed re-validation of round k: RESOLVED with round k in
-- the ledger history.
resolved AS (
    SELECT DISTINCT dataset, run_id, round AS revalidated_round, record_id
    FROM dq.quarantine_history
    WHERE status = 'RESOLVED'
),

resolved_by_action AS (
    SELECT
        rounds.dataset,
        rounds.run_id,
        rounds.round,
        count(*) FILTER (WHERE actions.fixed) AS manually_fixed,
        count(*) FILTER (WHERE actions.accepted AND NOT actions.fixed) AS confirmed
    FROM rounds
    JOIN resolved
      ON resolved.dataset = rounds.dataset
     AND resolved.run_id = rounds.run_id
     AND resolved.revalidated_round = rounds.revalidated_round
    JOIN actions
      ON actions.dataset = resolved.dataset
     AND actions.run_id = resolved.run_id
     AND actions.round = resolved.revalidated_round
     AND actions.record_id = resolved.record_id
    WHERE rounds.round > 1
    GROUP BY rounds.dataset, rounds.run_id, rounds.round
),

-- What each revalidation round did (null for round 1).
round_counts AS (
    SELECT
        rounds.dataset,
        rounds.run_id,
        rounds.round,
        rounds.round_type,
        rounds.revalidated_round,
        rounds.started_at,
        rounds.finished_at,
        CASE WHEN rounds.round > 1 THEN (rounds.counts ->> 'open_before')::bigint END
            AS round_open_before,
        CASE WHEN rounds.round > 1 THEN (rounds.counts ->> 'revalidated')::bigint END
            AS round_revalidated,
        CASE WHEN rounds.round > 1 THEN (rounds.counts ->> 'resolved')::bigint END
            AS round_resolved,
        CASE WHEN rounds.round > 1 THEN (rounds.counts ->> 'failed_again')::bigint END
            AS round_failed_again,
        CASE WHEN rounds.round > 1 THEN (rounds.counts ->> 'manual_review_required')::bigint END
            AS round_manual_review,
        CASE WHEN rounds.round > 1
            THEN (rounds.counts ->> 'discarded')::bigint
                 + (rounds.counts ->> 'removed_as_duplicate')::bigint
        END AS round_discarded,
        CASE WHEN rounds.round > 1 THEN (rounds.counts ->> 'open_after')::bigint END
            AS round_open_after,
        coalesce(resolved_by_action.manually_fixed, 0) AS round_manually_fixed,
        coalesce(resolved_by_action.confirmed, 0) AS round_confirmed
    FROM rounds
    LEFT JOIN resolved_by_action USING (dataset, run_id, round)
),

running AS (
    SELECT
        round_counts.*,
        sum(coalesce(round_resolved, 0)) OVER run_so_far AS resolved,
        sum(round_manually_fixed) OVER run_so_far AS manually_fixed_records,
        sum(round_confirmed) OVER run_so_far AS confirmed_records,
        sum(coalesce(round_discarded, 0)) OVER run_so_far AS discarded,
        sum(coalesce(round_manual_review, 0)) OVER run_so_far AS manual_review,
        round = max(round) OVER (PARTITION BY dataset, run_id) AS is_latest_round
    FROM round_counts
    WINDOW run_so_far AS (
        PARTITION BY dataset, run_id ORDER BY round
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
)

SELECT
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
    coalesce(auto_fixed.auto_fixed_records, 0)::bigint AS auto_fixed_records,
    first_round.quarantined,
    (coalesce(auto_fixed.auto_fixed_records, 0) + first_round.quarantined)::bigint
        AS records_with_issues,
    -- part / whole rounded to 6 decimals, 0 when whole is 0
    CASE WHEN first_round.processed > 0
        THEN round(first_round.valid::numeric / first_round.processed, 6)
        ELSE 0
    END AS initial_dq_score,
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
    running.resolved::bigint AS resolved,
    running.manually_fixed_records::bigint AS manually_fixed_records,
    running.confirmed_records::bigint AS confirmed_records,
    (running.resolved - running.manually_fixed_records - running.confirmed_records)::bigint
        AS auto_resolved_records,
    running.discarded::bigint AS discarded,
    running.manual_review::bigint AS manual_review,
    -- Open: everything quarantined after round 1, then what the latest
    -- re-validation left open.
    coalesce(running.round_open_after, first_round.quarantined) AS open,
    (coalesce(running.round_open_after, first_round.quarantined) + running.manual_review)::bigint
        AS unresolved_records,
    CASE WHEN first_round.processed > 0
        THEN round((first_round.valid + running.resolved)::numeric / first_round.processed, 6)
        ELSE 0
    END AS final_dq_score,
    running.is_latest_round
FROM running
JOIN first_round USING (dataset, run_id)
LEFT JOIN auto_fixed USING (dataset, run_id);

COMMENT ON VIEW metrics.dq_run IS
    'One row per run and round: record counts and DQ scores. Counts after round 1 are running totals; is_latest_round marks the run''s current figures.';
COMMENT ON COLUMN metrics.dq_run.processed IS 'Rows validated (input minus exact duplicates).';
COMMENT ON COLUMN metrics.dq_run.valid IS 'Accepted on the first validation.';
COMMENT ON COLUMN metrics.dq_run.auto_fixed_records IS 'Valid records that needed at least one automatic repair.';
COMMENT ON COLUMN metrics.dq_run.quarantined IS 'Failed the first validation.';
COMMENT ON COLUMN metrics.dq_run.records_with_issues IS 'auto_fixed_records + quarantined.';
COMMENT ON COLUMN metrics.dq_run.initial_dq_score IS 'valid / processed.';
COMMENT ON COLUMN metrics.dq_run.resolved IS 'Quarantined records RESOLVED so far.';
COMMENT ON COLUMN metrics.dq_run.manually_fixed_records IS 'Quarantined, corrected by hand (FIX) and RESOLVED.';
COMMENT ON COLUMN metrics.dq_run.confirmed_records IS 'Held because of many automatic repairs, accepted as they were by a person (ACCEPT) and RESOLVED.';
COMMENT ON COLUMN metrics.dq_run.auto_resolved_records IS 'RESOLVED without a manual fix or confirmation (re-validated with --all, e.g. after a rule change).';
COMMENT ON COLUMN metrics.dq_run.discarded IS 'Discarded by a person or removed as a duplicate.';
COMMENT ON COLUMN metrics.dq_run.unresolved_records IS 'Still OPEN or MANUAL_REVIEW_REQUIRED.';
COMMENT ON COLUMN metrics.dq_run.final_dq_score IS '(valid + resolved) / processed.';
COMMENT ON COLUMN metrics.dq_run.is_latest_round IS 'The run''s most recent round (its current figures).';

-- ---- dq_rule_result -----------------------------------------------------

CREATE VIEW metrics.dq_rule_result AS
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
FROM dq.validated_round AS rounds
CROSS JOIN LATERAL jsonb_array_elements(coalesce(rounds.result -> 'rules', '[]'::jsonb)) AS rule;

COMMENT ON VIEW metrics.dq_rule_result IS
    'One row per run, round and rule: checked, failed, repaired and unresolved values. Re-validation rounds count the corrected records only.';
COMMENT ON COLUMN metrics.dq_rule_result.dimension IS 'DQ dimension (validity, completeness, consistency, ...).';

-- ---- dq_quarantine ------------------------------------------------------

CREATE VIEW metrics.dq_quarantine AS
SELECT
    history.run_id,
    history.dataset,
    history.record_id,
    (row_number() OVER by_record)::integer AS sequence,
    history.round,
    history.status,
    lag(history.status) OVER by_record AS previous_status,
    history.attempts,
    history.changed_at
FROM dq.quarantine_history AS history
JOIN (SELECT DISTINCT dataset, run_id FROM dq.validated_round) AS runs USING (dataset, run_id)
WINDOW by_record AS (
    PARTITION BY history.dataset, history.run_id, history.record_id
    ORDER BY history.history_id
);

COMMENT ON VIEW metrics.dq_quarantine IS
    'Every status change of every quarantined record, numbered per record.';

-- ---- dq_correction ------------------------------------------------------

CREATE VIEW metrics.dq_correction AS
WITH rounds AS (
    SELECT dataset, run_id, round, revalidated_round, finished_at
    FROM dq.validated_round
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
    FROM dq.remediation_audit AS audit
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
    FROM dq.quarantine_history AS history
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
    FROM dq.correction_log AS log
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
FROM manual;

COMMENT ON VIEW metrics.dq_correction IS
    'Every automatic repair (remediation audit) and manual correction (correction log), with its outcome.';
