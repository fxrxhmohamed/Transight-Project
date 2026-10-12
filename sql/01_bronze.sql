-- Bronze layer: every ingested source file, unchanged, in PostgreSQL.
--
--   bronze.ingestion    one row per ingestion: what was loaded, from where,
--                       when, its SHA-256, row count, columns, profile
--   bronze.<dataset>    one table per dataset (created by the first
--                       ingestion, see bronze.py): one row per source row,
--                       one text column per source column, values exactly
--                       as read, plus ingestion_id, row_number, record_id
--
-- The layer is append-only: raw rows can never be updated or deleted, and
-- an ingestion only ever gets its profile filled in afterwards.
--
-- Docker runs this file once, when the database volume is first created.
-- Manually: psql -U metaflow -d metaflow -f sql/01_bronze.sql
-- (safe to run again).

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.ingestion (
    ingestion_id      text        PRIMARY KEY
                                  CHECK (ingestion_id ~ '^ING_[0-9]{8}_[0-9]{3}$'),
    dataset           text        NOT NULL,
    source_path       text        NOT NULL,
    source_file       text        NOT NULL,
    sha256            char(64)    NOT NULL,
    size_bytes        bigint      NOT NULL,
    encoding          text        NOT NULL,
    delimiter         text        NOT NULL,
    row_count         integer     NOT NULL,
    columns           text[]      NOT NULL,   -- source header, in order
    table_name        text        NOT NULL,   -- bronze.<table_name> holds the rows
    record_id_prefix  text        NOT NULL,   -- <ingestion id>_ROW_
    record_id_width   integer     NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    profile           jsonb                   -- filled in by the profiler
);

CREATE INDEX IF NOT EXISTS ingestion_dataset_idx
    ON bronze.ingestion (dataset, ingestion_id);

-- Databases created before per-dataset tables: rows of their earlier
-- ingestions stay in bronze.raw_record (JSON); re-ingest to get columns.
ALTER TABLE bronze.ingestion ADD COLUMN IF NOT EXISTS table_name text;

-- ---- Immutability -------------------------------------------------------
-- bronze.py attaches refuse_change to every dataset table it creates.

CREATE OR REPLACE FUNCTION bronze.refuse_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'bronze.% is append-only: % refused', TG_TABLE_NAME, TG_OP;
END;
$$;

CREATE OR REPLACE FUNCTION bronze.ingestion_profile_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (to_jsonb(NEW) - 'profile') IS DISTINCT FROM (to_jsonb(OLD) - 'profile') THEN
        RAISE EXCEPTION 'bronze.ingestion: only the profile of % may change',
            OLD.ingestion_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS ingestion_no_delete ON bronze.ingestion;
CREATE TRIGGER ingestion_no_delete
    BEFORE DELETE ON bronze.ingestion
    FOR EACH ROW EXECUTE FUNCTION bronze.refuse_change();

DROP TRIGGER IF EXISTS ingestion_no_truncate ON bronze.ingestion;
CREATE TRIGGER ingestion_no_truncate
    BEFORE TRUNCATE ON bronze.ingestion
    FOR EACH STATEMENT EXECUTE FUNCTION bronze.refuse_change();

DROP TRIGGER IF EXISTS ingestion_profile_only ON bronze.ingestion;
CREATE TRIGGER ingestion_profile_only
    BEFORE UPDATE ON bronze.ingestion
    FOR EACH ROW EXECUTE FUNCTION bronze.ingestion_profile_only();

-- ---- Convenience view ---------------------------------------------------

DROP VIEW IF EXISTS bronze.ingestion_summary;
CREATE VIEW bronze.ingestion_summary AS
SELECT ingestion_id, dataset, 'bronze.' || table_name AS stored_in,
       source_file, row_count, cardinality(columns) AS column_count, sha256,
       ingested_at, profile IS NOT NULL AS profiled
FROM bronze.ingestion;
