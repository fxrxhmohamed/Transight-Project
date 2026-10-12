-- Validated runs: (dataset, run_id) with a round 1 result.

select distinct dataset, run_id
from {{ ref('stg_dq__rounds') }}
