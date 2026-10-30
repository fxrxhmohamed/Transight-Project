-- Every run has exactly one latest round in dq_run.

{{ config(tags=['dq']) }}

select dataset, run_id
from {{ ref('dq_run') }}
group by dataset, run_id
having count(*) filter (where is_latest_round) <> 1
