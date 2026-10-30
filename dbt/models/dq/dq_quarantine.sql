-- Every status change of every quarantined record, numbered per record.

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
FROM {{ source('dq', 'quarantine_history') }} AS history
JOIN (SELECT DISTINCT dataset, run_id FROM {{ ref('dq_validated_round') }}) AS runs USING (dataset, run_id)
WINDOW by_record AS (
    PARTITION BY history.dataset, history.run_id, history.record_id
    ORDER BY history.history_id
)
