-- Semantic layer: the metrics of dbt/MetricFlow (business and data
-- quality), stored in PostgreSQL for Power BI and SQL users.
--
--   semantic.metric_definition   one row per metric (dbt/models/semantic/
--                                _metrics.yml): label, description, type,
--                                formula, unit, area, headline
--   semantic.metric_value        one row per metric and period: its value
--                                per month and in total, as MetricFlow
--                                computes it from gold and the DQ tables;
--                                the data-quality metrics also per run
--   semantic.metric_report       the values with their labels
--
-- Written by services/etl-service/src/semantic.py (python dq.py semantic)
-- in one transaction. dbt also builds in this schema the DQ tables
-- (dq_run, dq_rule_result, dq_quarantine, dq_correction: python dq.py
-- metrics), the views MetricFlow reads (fct_transactions, dim_*, dq_runs,
-- dq_rule_checks) and the time spine.
--
-- Docker runs this file once, when the database volume is first created;
-- the semantic step applies it again on every run (so a change here
-- reaches the database). Manually: psql -U metaflow -d metaflow -f
-- sql/05_semantic.sql (safe to run again).

CREATE SCHEMA IF NOT EXISTS semantic;

CREATE TABLE IF NOT EXISTS semantic.metric_definition (
    metric        text        PRIMARY KEY,
    label         text        NOT NULL,
    description   text,
    metric_type   text        NOT NULL,   -- simple | ratio | derived | ...
    formula       text        NOT NULL,   -- measure, ratio or expression, with its filter
    unit          text,                   -- count | EGP | rate
    area          text,                   -- Data Quality, Digital Adoption, ...
    headline      boolean     NOT NULL,   -- one of the headline metrics
    defined_at    timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS semantic.metric_value (
    metric        text        NOT NULL REFERENCES semantic.metric_definition ON DELETE CASCADE,
    grain         text        NOT NULL,   -- month | total | run
    period        date        NOT NULL,   -- month: its first day; run: the run's day;
                                          -- total: 0001-01-01
    run_id        text,                   -- grain run only
    value         numeric,                -- null when undefined (e.g. no prior year)
    computed_at   timestamptz NOT NULL,
    CONSTRAINT metric_value_grain CHECK (
        grain IN ('month', 'total', 'run') AND (run_id IS NOT NULL) = (grain = 'run')
    )
);

-- Tables made before the run grain: (metric, grain, period) was the key.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'semantic' AND table_name = 'metric_value'
          AND column_name = 'run_id'
    ) THEN
        ALTER TABLE semantic.metric_value
            DROP CONSTRAINT IF EXISTS metric_value_pkey,
            DROP CONSTRAINT IF EXISTS metric_value_grain_check,
            ADD COLUMN run_id text,
            ADD CONSTRAINT metric_value_grain CHECK (
                grain IN ('month', 'total', 'run') AND (run_id IS NOT NULL) = (grain = 'run')
            );
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS metric_value_key
    ON semantic.metric_value (metric, grain, period, coalesce(run_id, ''));

-- Dropped and created again: CREATE OR REPLACE VIEW cannot reorder columns.
DROP VIEW IF EXISTS semantic.metric_report;

CREATE VIEW semantic.metric_report AS
SELECT d.area, d.metric, d.label, d.unit, d.headline, v.grain,
       CASE WHEN v.grain = 'month' THEN v.period END AS month,
       v.run_id,
       CASE WHEN v.grain = 'run' THEN v.period END AS run_date,
       v.value, v.computed_at, d.description
FROM semantic.metric_value v
JOIN semantic.metric_definition d USING (metric);
