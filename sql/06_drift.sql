CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.drift_check (
    dataset          text        NOT NULL,
    run_id           text        NOT NULL,   -- the new file's ingestion id
    previous_run_id  text,                   -- null: first file, nothing to compare
    status           text        NOT NULL CHECK (status IN ('BASELINE','OK','WARNING','CRITICAL')),
    finding_count    integer     NOT NULL,
    emailed          boolean     NOT NULL DEFAULT false,
    checked_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, run_id)
);

CREATE TABLE IF NOT EXISTS dq.drift_finding (
    finding_id       bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset          text        NOT NULL,
    run_id           text        NOT NULL,
    previous_run_id  text        NOT NULL,
    check_name       text        NOT NULL,   -- row_count | null_rate | new_category | mean_change
    column_name      text,
    severity         text        NOT NULL CHECK (severity IN ('warning','critical')),
    old_value        text,
    new_value        text,
    change           numeric,
    message          text        NOT NULL,
    detected_at      timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (dataset, run_id) REFERENCES dq.drift_check ON DELETE CASCADE
);