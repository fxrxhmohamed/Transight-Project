-- One row per day, for MetricFlow (time dimensions and cumulative
-- metrics). Covers the transaction dates and the pipeline's runs.

select day::date as date_day
from generate_series('2015-01-01'::date, '2040-12-31'::date, interval '1 day') as day
