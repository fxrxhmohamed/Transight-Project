-- Data-quality layer: everything after bronze, in PostgreSQL.
--
--   dq.run                  one row per validated run (ingestion): the
--                           record columns of its outputs
--   dq.round_result         summary of every round (validation = round 1)
--   dq.quarantine_status    the ledger: one row per quarantined record
--   dq.quarantine_history   every status change of the ledger, in order
--   dq.quarantine_error     one row per failed rule, record and round
--   dq.remediation_audit    every automatic repair, per round
--   dq.correction           manual corrections to apply, per round
--   dq.correction_log       every manual correction ever imported
--   dq.import_rejection     correction lines that were refused
--   dq.merge_result         counts and checks of the latest merge of a run
--
--   metrics.*               the DQ metric views for Power BI
--                           (sql/04_metrics.sql, python dq.py metrics)
--
-- Record tables, one per dataset, are created by the dq-service
-- (layout.py), one text column per data column like bronze:
--
--   dq.<dataset>_valid       records that passed the first validation
--   dq.<dataset>_quarantine  records open in each round (cleaned values)
--   dq.<dataset>_resolved    records that passed re-validation, per round
--   silver.<dataset>         clean data: valid + resolved, one row per record
--
-- Docker runs this file once, when the database volume is first created;
-- the dq-service also applies it when the dq schema is missing.
-- Manually: psql -U metaflow -d metaflow -f sql/02_dq.sql (safe to run again).

CREATE SCHEMA IF NOT EXISTS dq;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS metrics;

-- ---- Runs ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dq.run (
    dataset       text        NOT NULL,
    run_id        text        NOT NULL,   -- ingestion id, or a timestamp
    source        text,
    columns       text[]      NOT NULL,   -- record columns, in order
    validated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, run_id)
);

CREATE TABLE IF NOT EXISTS dq.round_result (
    dataset  text    NOT NULL,
    run_id   text    NOT NULL,
    round    integer NOT NULL CHECK (round > 0),
    result   jsonb   NOT NULL,
    PRIMARY KEY (dataset, run_id, round),
    FOREIGN KEY (dataset, run_id) REFERENCES dq.run ON DELETE CASCADE
);

-- ---- Quarantine ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS dq.quarantine_status (
    dataset     text        NOT NULL,
    run_id      text        NOT NULL,
    record_id   text        NOT NULL,
    status      text        NOT NULL CHECK (status IN (
                    'OPEN', 'RESOLVED', 'MANUAL_REVIEW_REQUIRED', 'DISCARDED')),
    round       integer     NOT NULL,
    attempts    integer     NOT NULL,
    updated_at  timestamptz NOT NULL,
    PRIMARY KEY (dataset, run_id, record_id),
    FOREIGN KEY (dataset, run_id) REFERENCES dq.run ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dq.quarantine_history (
    history_id  bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset     text        NOT NULL,
    run_id      text        NOT NULL,
    record_id   text        NOT NULL,
    status      text        NOT NULL,
    round       integer     NOT NULL,
    attempts    integer     NOT NULL,
    changed_at  timestamptz NOT NULL,
    FOREIGN KEY (dataset, run_id) REFERENCES dq.run ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS quarantine_history_run_idx
    ON dq.quarantine_history (dataset, run_id, history_id);

CREATE TABLE IF NOT EXISTS dq.quarantine_error (
    dataset         text    NOT NULL,
    run_id          text    NOT NULL,
    round           integer NOT NULL,
    record_id       text    NOT NULL,
    rule_id         text    NOT NULL,
    rule_name       text,
    error_code      text,
    error_category  text,
    dimension       text,
    severity        text,
    "column"        text,
    value           text,
    message         text,
    FOREIGN KEY (dataset, run_id) REFERENCES dq.run ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS quarantine_error_run_idx
    ON dq.quarantine_error (dataset, run_id, round, record_id);

-- ---- Repairs and corrections --------------------------------------------

CREATE TABLE IF NOT EXISTS dq.remediation_audit (
    dataset         text    NOT NULL,
    run_id          text    NOT NULL,
    round           integer NOT NULL,
    record_id       text    NOT NULL,
    "column"        text,
    original_value  text,
    cleaned_value   text,
    rule_id         text,
    action          text,
    outcome         text,
    FOREIGN KEY (dataset, run_id) REFERENCES dq.run ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS remediation_audit_run_idx
    ON dq.remediation_audit (dataset, run_id, round, record_id);

-- Corrections to apply when a round is re-validated: the newest line per
-- record, column and action ("column" is empty for DISCARD and ACCEPT).
CREATE TABLE IF NOT EXISTS dq.correction (
    dataset       text        NOT NULL,
    run_id        text        NOT NULL,
    round         integer     NOT NULL,
    record_id     text        NOT NULL,
    "column"      text,
    old_value     text,
    new_value     text,
    action        text        NOT NULL CHECK (action IN ('FIX', 'DISCARD', 'ACCEPT')),
    reason        text,
    "user"        text,
    corrected_at  timestamptz NOT NULL,
    FOREIGN KEY (dataset, run_id) REFERENCES dq.run ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS correction_run_idx
    ON dq.correction (dataset, run_id, round);

CREATE TABLE IF NOT EXISTS dq.correction_log (
    log_id        bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset       text        NOT NULL,
    run_id        text        NOT NULL,
    round         integer     NOT NULL,
    record_id     text        NOT NULL,
    "column"      text,
    old_value     text,
    new_value     text,
    action        text        NOT NULL,
    reason        text,
    "user"        text,
    corrected_at  timestamptz NOT NULL,
    FOREIGN KEY (dataset, run_id) REFERENCES dq.run ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS correction_log_run_idx
    ON dq.correction_log (dataset, run_id, log_id);

CREATE TABLE IF NOT EXISTS dq.import_rejection (
    dataset      text        NOT NULL,
    run_id       text        NOT NULL,
    round        integer     NOT NULL,
    record_id    text,
    "column"     text,
    new_value    text,
    action       text,
    reason       text,
    refused      text        NOT NULL,   -- why the line was refused
    rejected_at  timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (dataset, run_id) REFERENCES dq.run ON DELETE CASCADE
);

-- ---- Merge --------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dq.merge_result (
    dataset       text        NOT NULL,
    run_id        text        NOT NULL,
    merged_at     timestamptz NOT NULL,
    published_by  text,
    result        jsonb       NOT NULL,
    PRIMARY KEY (dataset, run_id),
    FOREIGN KEY (dataset, run_id) REFERENCES dq.run ON DELETE CASCADE
);
