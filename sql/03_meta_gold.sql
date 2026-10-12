-- Metadata engine and gold layer.
--
--   meta.dataset        one row per dataset: its silver table, rows, runs,
--                       grain, primary key, when the metadata was generated
--   meta.column         one row per silver column: type, role, entity, PII,
--                       description, and what the data holds (nulls,
--                       distinct values, min, max)
--   meta.entity         one row per entity: key, attributes, distinct keys
--   meta.relationship   one row per reference between entities, with the
--                       keys whose reference is empty
--
-- Written by services/metadata-service/src/metadata.py from silver and the
-- schema registry; read by the AutoETL step (services/etl-service/src/
-- gold.py), which builds the gold star schema from it:
--
--   gold.fact_<grain entity>   one row per record of the grain (latest run)
--   gold.dim_<entity>          one row per key of every other entity
--   gold.dim_date              one row per day the facts cover
--
-- Gold tables are created by gold.py (their columns follow the metadata)
-- and replaced in one transaction on every build.
--
-- Docker runs this file once, when the database volume is first created;
-- the steps also apply it when the meta schema is missing.
-- Manually: psql -U metaflow -d metaflow -f sql/03_meta_gold.sql (safe to run again).

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS meta.dataset (
    dataset         text        PRIMARY KEY,
    description     text,
    silver_table    text        NOT NULL,
    grain           text,
    grain_entity    text,
    primary_key     text[],
    row_count       bigint      NOT NULL,
    record_count    bigint      NOT NULL,   -- rows after keeping the latest run per key
    run_count       integer     NOT NULL,
    schema_version  text,
    generated_at    timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS meta.column (
    dataset         text    NOT NULL REFERENCES meta.dataset ON DELETE CASCADE,
    column_name     text    NOT NULL,
    ordinal         integer NOT NULL,
    logical_type    text    NOT NULL,   -- schema registry type
    database_type   text    NOT NULL,   -- type in silver
    role            text,
    entity          text,
    is_key          boolean NOT NULL,
    is_pii          boolean NOT NULL,
    generated       boolean NOT NULL,
    nullable        text,
    unit            text,
    description     text,
    null_count      bigint  NOT NULL,
    null_rate       numeric NOT NULL,
    distinct_count  bigint  NOT NULL,   -- approximate (HyperLogLog, ~2% error)
    min_value       text,
    max_value       text,
    PRIMARY KEY (dataset, column_name)
);

CREATE TABLE IF NOT EXISTS meta.entity (
    dataset         text    NOT NULL REFERENCES meta.dataset ON DELETE CASCADE,
    entity          text    NOT NULL,
    key_column      text    NOT NULL,
    attributes      text[]  NOT NULL,
    is_grain        boolean NOT NULL,
    description     text,
    distinct_keys   bigint  NOT NULL,
    PRIMARY KEY (dataset, entity)
);

CREATE TABLE IF NOT EXISTS meta.relationship (
    dataset         text    NOT NULL REFERENCES meta.dataset ON DELETE CASCADE,
    from_entity     text    NOT NULL,
    from_column     text    NOT NULL,
    to_entity       text    NOT NULL,
    to_column       text    NOT NULL,
    null_references bigint  NOT NULL,   -- from_entity keys with no reference
    PRIMARY KEY (dataset, from_entity, from_column)
);
