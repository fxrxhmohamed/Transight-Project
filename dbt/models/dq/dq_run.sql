-- One row per run and round: record counts and DQ scores. Counts after
-- round 1 are as of the end of that round (running totals over the
-- rounds), so the latest round of a run reads e.g. 350 issues -> 200
-- auto-fixed -> 130 manually fixed -> 20 unresolved (is_latest_round).

WITH rounds AS (
    SELECT * FROM {{ ref('dq_validated_round') }}
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
    FROM {{ source('dq', 'remediation_audit') }} AS audit
    WHERE audit.round = 1
      AND audit.outcome IN ('remediated', 'nullified_in_accepted')
      AND NOT EXISTS (
          SELECT 1
          FROM {{ source('dq', 'quarantine_status') }} AS ledger
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
    FROM {{ source('dq', 'correction') }}
    WHERE action IN ('FIX', 'ACCEPT')
    GROUP BY dataset, run_id, round, record_id
),

-- Records that passed re-validation of round k: RESOLVED with round k in
-- the ledger history.
resolved AS (
    SELECT DISTINCT dataset, run_id, round AS revalidated_round, record_id
    FROM {{ source('dq', 'quarantine_history') }}
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
LEFT JOIN auto_fixed USING (dataset, run_id)
