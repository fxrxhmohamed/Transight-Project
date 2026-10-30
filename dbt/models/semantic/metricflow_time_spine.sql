-- One row per day, for MetricFlow (time grains and offset metrics such as
-- year-over-year growth).

select day::date as date_day
from generate_series('2015-01-01'::date, '2040-12-31'::date, interval '1 day') as day
