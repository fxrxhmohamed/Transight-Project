-- Every round of every validated run (a run is validated once its round 1
-- result is stored), with the fields of its JSON summary.

with results as (

    select dataset, run_id, round, result
    from {{ source('dq', 'round_result') }}

),

validated as (

    select dataset, run_id
    from results
    where round = 1

)

select
    results.dataset,
    results.run_id,
    results.round,
    case when results.round = 1 then 'validation' else 'revalidation' end as round_type,
    (results.result ->> 'revalidated_round')::integer as revalidated_round,
    (results.result ->> 'started_at')::timestamptz as started_at,
    (results.result ->> 'finished_at')::timestamptz as finished_at,
    results.result -> 'counts' as counts,
    results.result
from results
join validated using (dataset, run_id)
