{{ config(indexes=[{'columns': ['dataset', 'run_id', 'record_id']}]) }}

-- Every status change of every quarantined record, numbered per record.

select
    history.run_id,
    history.dataset,
    history.record_id,
    (row_number() over by_record)::integer as sequence,
    history.round,
    history.status,
    lag(history.status) over by_record as previous_status,
    history.attempts,
    history.changed_at
from {{ source('dq', 'quarantine_history') }} as history
join {{ ref('stg_dq__runs') }} using (dataset, run_id)
window by_record as (
    partition by history.dataset, history.run_id, history.record_id
    order by history.history_id
)
