-- Every round of every validated run (a run with a round 1 result), with
-- the fields of its JSON summary: the shared input of the DQ models.
-- Ephemeral: inlined into them, not stored.

{{ config(materialized='ephemeral') }}

WITH validated AS (
    SELECT dataset, run_id
    FROM {{ source('dq', 'round_result') }}
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
FROM {{ source('dq', 'round_result') }} AS results
JOIN validated USING (dataset, run_id)
