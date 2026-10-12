-- Every quarantined record is resolved, discarded or still unresolved, in
-- every round (rows returned = failures).

select dataset, run_id, round, quarantined, resolved, discarded, unresolved_records
from {{ ref('dq_run') }}
where resolved + discarded + unresolved_records <> quarantined
