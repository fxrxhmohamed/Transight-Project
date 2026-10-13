"""Where the data-quality outputs of a run live: PostgreSQL (sql/02_dq.sql).

A run is one ingestion (its ingestion id) or an ad hoc file (a timestamp).

    dq.run                    the run and the columns of its records
    dq.<dataset>_valid        records that passed on the first validation
    dq.<dataset>_quarantine   records open in each round (cleaned values)
    dq.quarantine_error       one row per failed rule, record and round
    dq.quarantine_status      the ledger: one row per quarantined record
                              with its status
    dq.quarantine_history     every status change of the ledger, in order
    dq.correction             manual corrections to apply in each round
    dq.correction_log         every manual correction, all rounds
    dq.import_rejection       correction lines that were refused
    dq.<dataset>_resolved     records that passed re-validation, per round
    dq.remediation_audit      every automatic repair, per round
    dq.round_result           summary of every round
    silver.<dataset>          clean data, typed: valid records plus
                              resolved ones, one row per primary key (the
                              newest run's version; see load_into_silver)
    dq.merge_result           counts and checks of the latest merge
    dq.load_log               every load into silver (initial or
                              incremental) with its counts

Record values are stored as text, exactly as the engine writes them
(silver.<dataset> has typed columns, see merge.py). Round 1 is the first
validation; re-validating the corrections of round n produces round n + 1.

Spark writes the record tables, errors, audit and round-1 ledger over
JDBC; the small bookkeeping tables are written here in transactions. A
round counts once its result is in dq.round_result (written last), so an
interrupted step leaves nothing that counts and can simply run again.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from psycopg import sql
from psycopg.types.json import Jsonb

import db
from db import LayoutError  # noqa: F401  (re-exported)


# Record status in the quarantine ledger.
OPEN = "OPEN"
RESOLVED = "RESOLVED"
MANUAL_REVIEW = "MANUAL_REVIEW_REQUIRED"
DISCARDED = "DISCARDED"

STATUS_COLUMNS = ["record_id", "status", "round", "attempts", "updated_at"]
HISTORY_COLUMNS = ["record_id", "status", "round", "attempts", "changed_at"]

AUDIT_COLUMNS = [
    "record_id", "column", "original_value", "cleaned_value",
    "rule_id", "action", "outcome",
]
ERROR_COLUMNS = [
    "record_id", "rule_id", "rule_name", "error_code", "error_category",
    "dimension", "severity", "column", "value", "message",
]
CORRECTION_COLUMNS = [
    "record_id", "column", "old_value", "new_value", "action", "reason",
    "user", "corrected_at",
]
LOG_COLUMNS = ["round", *CORRECTION_COLUMNS]
REJECTION_COLUMNS = ["record_id", "column", "new_value", "action", "reason", "refused"]

# Engine-added columns, never fed back into validation.
DQ_COLUMNS = ("record_id", "dq_errors", "dq_warnings")

RUN = sql.Identifier("dq", "run")
ROUND_RESULT = sql.Identifier("dq", "round_result")
STATUS = sql.Identifier("dq", "quarantine_status")
HISTORY = sql.Identifier("dq", "quarantine_history")
ERRORS = sql.Identifier("dq", "quarantine_error")
AUDIT = sql.Identifier("dq", "remediation_audit")
CORRECTIONS = sql.Identifier("dq", "correction")
CORRECTION_LOG = sql.Identifier("dq", "correction_log")
REJECTIONS = sql.Identifier("dq", "import_rejection")
MERGE_RESULT = sql.Identifier("dq", "merge_result")
LOAD_LOG = sql.Identifier("dq", "load_log")

# Extra columns of each record table after the data columns.
RECORD_EXTRA = {
    "valid": ["dq_warnings"],
    "quarantine": ["dq_errors", "dq_warnings"],
    "resolved": ["dq_warnings"],
    "silver": ["dq_warnings"],
}


def _primary_key(conn, schema: str, name: str) -> list[str]:
    """Columns of the table's primary key, in order."""

    row = conn.execute(
        "SELECT array_agg(a.attname::text ORDER BY k.n) FROM pg_index i "
        "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, n) "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "
        "WHERE i.indrelid = to_regclass(%s) AND i.indisprimary",
        (db.qualified(schema, name),),
    ).fetchone()
    return list(row[0] or [])


def _add_silver_keys(conn, schema: str, name: str, key: list[str]) -> None:
    """Primary key, unique record ids and the load time of silver."""

    table = sql.Identifier(schema, name)
    conn.execute(sql.SQL(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS loaded_at timestamptz NOT NULL DEFAULT now()"
    ).format(table))
    conn.execute(sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
        table, sql.Identifier(f"{name}_key"[:63]),
        sql.SQL(", ").join(map(sql.Identifier, key))))
    conn.execute(sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} UNIQUE (record_id)").format(
        table, sql.Identifier(f"{name}_record"[:63])))


def _convert_silver(conn, schema: str, name: str, key: list[str]) -> None:
    """Silver of an earlier version (one row per run and record) -> one row
    per key, the newest run's; the old table is kept as <name>_by_run."""

    table = sql.Identifier(schema, name)
    old_name = f"{name}_by_run"[:63]
    old = sql.Identifier(schema, old_name)
    if db.table_columns(conn, schema, old_name):
        raise LayoutError(f"{schema}.{old_name} already exists; cannot convert "
                          f"{schema}.{name} to one row per key.")
    conn.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(
        table, sql.Identifier(old_name)))
    for (constraint,) in conn.execute(
        "SELECT conname::text FROM pg_constraint WHERE conrelid = to_regclass(%s) "
        "AND contype IN ('p', 'u')", (db.qualified(schema, old_name),),
    ).fetchall():
        conn.execute(sql.SQL("ALTER TABLE {} RENAME CONSTRAINT {} TO {}").format(
            old, sql.Identifier(constraint),
            sql.Identifier(f"{old_name}_{constraint}"[:63])))
    conn.execute(sql.SQL("CREATE TABLE {} (LIKE {})").format(table, old))
    keys = sql.SQL(", ").join(map(sql.Identifier, key))
    present = sql.SQL(" AND ").join(
        sql.SQL("{} IS NOT NULL").format(sql.Identifier(k)) for k in key)
    conn.execute(sql.SQL(
        "INSERT INTO {} SELECT DISTINCT ON ({}) * FROM {} WHERE {} "
        "ORDER BY {}, run_id DESC, record_id DESC"
    ).format(table, keys, old, present, keys))
    _add_silver_keys(conn, schema, name, key)


def _json(value: Any) -> Jsonb:
    return Jsonb(value, dumps=lambda v: json.dumps(v, ensure_ascii=False, default=str))


def _ids_condition(record_ids: set[str] | list[str]) -> sql.Composable:
    return sql.SQL("record_id = ANY({})").format(sql.Literal(sorted(record_ids)))


@dataclass(frozen=True)
class Layout:
    dataset: str
    run_id: str

    @property
    def key(self) -> dict[str, str]:
        return {"dataset": self.dataset, "run_id": self.run_id}

    # ---- The run ----

    def exists(self) -> bool:
        """The run is validated (its round 1 result is stored)."""

        with db.transaction() as conn:
            return conn.execute(
                "SELECT 1 FROM dq.round_result "
                "WHERE dataset = %s AND run_id = %s AND round = 1",
                (self.dataset, self.run_id),
            ).fetchone() is not None

    def columns(self) -> list[str]:
        """Data columns of the run's records, in order."""

        with db.transaction() as conn:
            row = conn.execute(
                "SELECT columns FROM dq.run WHERE dataset = %s AND run_id = %s",
                (self.dataset, self.run_id),
            ).fetchone()
        if row is None:
            raise LayoutError(
                f"Run {self.run_id} of {self.dataset} is not validated; validate it first."
            )
        return list(row[0])

    def validated_at(self) -> str:
        with db.transaction() as conn:
            row = conn.execute(
                "SELECT validated_at FROM dq.run WHERE dataset = %s AND run_id = %s",
                (self.dataset, self.run_id),
            ).fetchone()
        return row[0].isoformat() if row else ""

    def start(self, columns: list[str], source: str) -> None:
        """Register the run (after delete() when it is validated again)."""

        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO dq.run (dataset, run_id, source, columns) "
                "VALUES (%s, %s, %s, %s)",
                (self.dataset, self.run_id, source, columns),
            )

    def delete(self) -> None:
        """Remove every output of the run (rounds, corrections, clean data)."""

        with db.transaction() as conn:
            for kind in RECORD_EXTRA:
                if db.table_columns(conn, *db.record_table_name(self.dataset, kind)):
                    db.delete(conn, db.record_table(self.dataset, kind),
                              {"run_id": self.run_id})
            # The other dq tables follow through ON DELETE CASCADE.
            db.delete(conn, RUN, self.key)

    # ---- Tables Spark writes and reads ----

    def prepare_records(self, kind: str, round_number: int | None = None) -> str:
        """Create the record table (or add the columns it lacks), remove
        the run's rows (of one round) and return its name for Spark."""

        where: dict[str, object] = {"run_id": self.run_id}
        if round_number is not None:
            where["round"] = round_number
        with db.transaction() as conn:
            table = db.ensure_record_table(
                conn, self.dataset, kind, [*self.columns(), *RECORD_EXTRA[kind]],
            )
            db.delete(conn, table, where)
        return db.qualified(*db.record_table_name(self.dataset, kind))

    def has_records(self, kind: str) -> bool:
        """The dataset's record table of this kind exists."""

        with db.transaction() as conn:
            return bool(db.table_columns(conn, *db.record_table_name(self.dataset, kind)))

    def clear(self, table: sql.Identifier, round_number: int) -> None:
        """Remove the run's rows of one round from a dq table."""

        with db.transaction() as conn:
            db.delete(conn, table, {**self.key, "round": round_number})

    def spark_records(
        self,
        kind: str,
        round_number: int | None = None,
        where: str | None = None,
        with_round: bool = False,
    ):
        """A record table of the run as a Spark DataFrame: record_id, the
        data columns and the kind's extra columns."""

        wanted = ["record_id", *self.columns(), *RECORD_EXTRA[kind]]
        if with_round:
            wanted = ["round", *wanted]
        conditions = [f"run_id = {db.literal(self.run_id)}"]
        if round_number is not None:
            conditions.append(f"round = {int(round_number)}")
        if where:
            conditions.append(where)
        query = (
            f"SELECT {', '.join(db.quote(c) for c in wanted)} "
            f"FROM {db.qualified(*db.record_table_name(self.dataset, kind))} "
            f"WHERE {' AND '.join(conditions)}"
        )
        return db.spark_io.read(query, predicates=db.spark_io.hash_predicates("record_id"))

    def spark_table(self, table: str, columns: list[str], round_number: int | None = None,
                    where: str | None = None):
        """Rows of the run in a dq table (e.g. dq.quarantine_error)."""

        conditions = [f"dataset = {db.literal(self.dataset)}",
                      f"run_id = {db.literal(self.run_id)}"]
        if round_number is not None:
            conditions.append(f"round = {int(round_number)}")
        if where:
            conditions.append(where)
        query = (f"SELECT {', '.join(db.quote(c) for c in columns)} FROM {table} "
                 f"WHERE {' AND '.join(conditions)}")
        return db.spark_io.read(query, predicates=db.spark_io.hash_predicates("record_id"))

    # ---- Record tables ----

    def _save_records(
        self, kind: str, frame: pd.DataFrame, round_number: int | None = None,
    ) -> None:
        where = {"run_id": self.run_id}
        if round_number is not None:
            where["round"] = round_number
        with db.transaction() as conn:
            table = db.ensure_record_table(
                conn, self.dataset, kind,
                [*self.columns(), *RECORD_EXTRA[kind]],
            )
            db.delete(conn, table, where)
            db.write_frame(conn, table, frame, fixed=where)

    def _load_records(
        self,
        kind: str,
        round_number: int | None = None,
        columns: list[str] | None = None,
        record_ids: set[str] | None = None,
        with_round: bool = False,
    ) -> pd.DataFrame:
        wanted = [
            "record_id",
            *(columns if columns is not None else self.columns()),
            *(RECORD_EXTRA[kind] if columns is None else []),
        ]
        if with_round:
            wanted = ["round", *wanted]
        where: dict[str, object] = {"run_id": self.run_id}
        if round_number is not None:
            where["round"] = round_number

        with db.transaction() as conn:
            existing = db.table_columns(conn, *db.record_table_name(self.dataset, kind))
            if not existing:
                return pd.DataFrame({c: pd.Series(dtype="string") for c in wanted})
            # Columns of the run the table never received (no rows yet).
            selectable = [c for c in wanted if c in existing]
            frame = db.select(
                conn, db.record_table(self.dataset, kind), selectable, where,
                order_by=["record_id"],
                extra=None if record_ids is None else _ids_condition(record_ids),
            )
        return frame.reindex(columns=wanted)

    def save_valid(self, frame: pd.DataFrame) -> None:
        self._save_records("valid", frame)

    def load_valid(self, columns: list[str] | None = None) -> pd.DataFrame:
        """record_id, the data columns (or only columns) and dq_warnings."""

        return self._load_records("valid", columns=columns)

    def save_quarantine(
        self, round_number: int, records: pd.DataFrame, errors: pd.DataFrame,
    ) -> None:
        with db.transaction() as conn:
            self._save_records("quarantine", records, round_number)
            where = {**self.key, "round": round_number}
            db.delete(conn, ERRORS, where)
            db.write_frame(conn, ERRORS, errors[ERROR_COLUMNS], fixed=where)

    def load_quarantine(
        self,
        round_number: int,
        record_ids: set[str] | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Records of a round: record_id, data columns, dq_errors, dq_warnings
        (or record_id and only columns)."""

        return self._load_records(
            "quarantine", round_number, columns=columns, record_ids=record_ids,
        )

    def load_errors(self, round_number: int) -> pd.DataFrame:
        with db.transaction() as conn:
            return db.select(
                conn, ERRORS, ERROR_COLUMNS, {**self.key, "round": round_number},
                order_by=["record_id"],
            )

    def save_resolved(self, round_number: int, frame: pd.DataFrame) -> None:
        self._save_records("resolved", frame, round_number)

    def load_resolved(self) -> list[tuple[int, pd.DataFrame]]:
        """Resolved records of every round that has some, oldest first."""

        frame = self._load_records("resolved", with_round=True)
        if not len(frame):
            return []
        rounds = frame.pop("round").astype(int)
        return [
            (int(n), part.reset_index(drop=True))
            for n, part in frame.groupby(rounds, sort=True)
        ]

    # ---- Repairs and round summaries ----

    def save_audit(self, round_number: int, audit: pd.DataFrame) -> None:
        where = {**self.key, "round": round_number}
        with db.transaction() as conn:
            db.delete(conn, AUDIT, where)
            db.write_frame(conn, AUDIT, audit[AUDIT_COLUMNS], fixed=where)

    def load_audit(
        self,
        round_number: int | None = None,
        record_ids: set[str] | None = None,
        outcome: str | None = None,
    ) -> pd.DataFrame:
        """Automatic repairs of one round (default: every round)."""

        where: dict[str, object] = dict(self.key)
        if round_number is not None:
            where["round"] = round_number
        if outcome is not None:
            where["outcome"] = outcome
        with db.transaction() as conn:
            return db.select(
                conn, AUDIT, AUDIT_COLUMNS, where, order_by=["round", "record_id"],
                extra=None if record_ids is None else _ids_condition(record_ids),
            )

    def save_result(self, round_number: int, result: dict[str, Any]) -> None:
        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO dq.round_result (dataset, run_id, round, result) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT (dataset, run_id, round) "
                "DO UPDATE SET result = EXCLUDED.result",
                (self.dataset, self.run_id, round_number, _json(result)),
            )

    def load_results(self) -> dict[int, dict[str, Any]]:
        """Summary of every round, by round number."""

        with db.transaction() as conn:
            rows = conn.execute(
                "SELECT round, result FROM dq.round_result "
                "WHERE dataset = %s AND run_id = %s ORDER BY round",
                (self.dataset, self.run_id),
            ).fetchall()
        return {number: result for number, result in rows}

    # ---- Corrections ----

    def load_corrections(self, round_number: int) -> pd.DataFrame:
        with db.transaction() as conn:
            return db.select(
                conn, CORRECTIONS, CORRECTION_COLUMNS,
                {**self.key, "round": round_number},
            )

    def replace_corrections(self, round_number: int, frame: pd.DataFrame) -> None:
        where = {**self.key, "round": round_number}
        with db.transaction() as conn:
            db.delete(conn, CORRECTIONS, where)
            db.write_frame(conn, CORRECTIONS, frame[CORRECTION_COLUMNS], fixed=where)

    def append_log(self, round_number: int, frame: pd.DataFrame) -> None:
        with db.transaction() as conn:
            db.write_frame(
                conn, CORRECTION_LOG, frame[CORRECTION_COLUMNS],
                fixed={**self.key, "round": round_number},
            )

    def load_log(self, latest: int | None = None) -> pd.DataFrame:
        """The correction log in order (only the latest lines when given)."""

        query = sql.SQL(
            "SELECT {} FROM dq.correction_log WHERE dataset = {} AND run_id = {} "
            "ORDER BY log_id DESC"
        ).format(
            db.select_list(LOG_COLUMNS),
            sql.Literal(self.dataset), sql.Literal(self.run_id),
        )
        if latest is not None:
            query += sql.SQL(" LIMIT {}").format(sql.Literal(latest))
        with db.transaction() as conn:
            log = db.read_frame(conn, query, LOG_COLUMNS)
        return log.iloc[::-1].reset_index(drop=True)

    def save_rejections(self, round_number: int, refused: pd.DataFrame) -> None:
        with db.transaction() as conn:
            db.write_frame(
                conn, REJECTIONS, refused[REJECTION_COLUMNS],
                fixed={**self.key, "round": round_number},
            )

    # ---- Clean data ----

    def prepare_silver(self, types: dict[str, str], key: list[str]) -> str:
        """Create silver.<dataset> keyed by the primary key (or add the
        columns it lacks) and an empty staging table for the run's rows;
        returns the staging table's name for Spark.

        Silver made by earlier versions (one row per run and record) is
        converted once: the newest version of every key is kept and the old
        table stays as silver.<dataset>_by_run.
        """

        schema, name = db.record_table_name(self.dataset, "silver")
        table = sql.Identifier(schema, name)
        with db.transaction() as conn:
            existing = db.table_columns(conn, schema, name)
            if existing and _primary_key(conn, schema, name) != key:
                _convert_silver(conn, schema, name, key)
                existing = db.table_columns(conn, schema, name)
            if not existing:
                conn.execute(sql.SQL(
                    "CREATE TABLE {} (run_id text NOT NULL, record_id text NOT NULL)"
                ).format(table))
            for column, kind in {**types, "dq_warnings": "text"}.items():
                if column not in existing:
                    conn.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}")
                                 .format(table, sql.Identifier(column), sql.SQL(kind)))
            if not existing:
                _add_silver_keys(conn, schema, name, key)

            stage = f"_stage_{uuid.uuid4().hex[:12]}"
            conn.execute(sql.SQL("CREATE TABLE {} (LIKE {})").format(
                sql.Identifier(schema, stage), table))
            conn.execute(sql.SQL("ALTER TABLE {} DROP COLUMN loaded_at").format(
                sql.Identifier(schema, stage)))
        return db.qualified(schema, stage)

    def drop_stage(self, stage: str) -> None:
        schema, _ = db.record_table_name(self.dataset, "silver")
        name = stage.split(".", 1)[1].strip('"')
        with db.transaction() as conn:
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(
                sql.Identifier(schema, name)))

    def load_into_silver(self, stage: str, key: list[str]) -> dict[str, Any]:
        """Upsert the staged rows of the run into silver by primary key, in
        one transaction, and log the load in dq.load_log.

        A staged row is inserted (new key), updated (its values differ and
        silver's version is not from a newer run), unchanged (same values;
        silver keeps its row and lineage) or older (silver holds a newer
        run's version, which stays). Rows of this run that the staged rows
        no longer hold (an earlier merge of it) are removed. The counts
        must add up, or nothing is loaded.
        """

        schema, name = db.record_table_name(self.dataset, "silver")
        table = sql.Identifier(schema, name)
        staged_table = sql.Identifier(schema, stage.split(".", 1)[1].strip('"'))
        with db.transaction() as conn:
            conn.execute(sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(table))
            columns = db.table_columns(conn, schema, name)
            data = [c for c in columns if c not in ("loaded_at", *key)]
            changing = [c for c in data if c not in ("run_id", "record_id")]
            on_key = sql.SQL(" AND ").join(
                sql.SQL("s.{0} = t.{0}").format(sql.Identifier(k)) for k in key)

            def count(query: str, *parts: sql.Composable) -> int:
                return conn.execute(sql.SQL(query).format(*parts)).fetchone()[0]

            before = count("SELECT count(*) FROM {}", table)
            staged = count("SELECT count(*) FROM {}", staged_table)
            older = count("SELECT count(*) FROM {} s JOIN {} t ON {} "
                          "WHERE t.run_id > s.run_id", staged_table, table, on_key)
            removed = conn.execute(sql.SQL(
                "DELETE FROM {} t WHERE t.run_id = {} AND NOT EXISTS "
                "(SELECT 1 FROM {} s WHERE s.record_id = t.record_id)"
            ).format(table, sql.Literal(self.run_id), staged_table)).rowcount

            names = sql.SQL(", ").join(map(sql.Identifier, [*key, *data]))
            inserted, updated = conn.execute(sql.SQL(
                "WITH loaded AS ("
                " INSERT INTO {table} AS t ({names}, loaded_at)"
                " SELECT {names}, now() FROM {stage}"
                " ON CONFLICT ({key}) DO UPDATE SET {sets}, loaded_at = now()"
                " WHERE t.run_id <= EXCLUDED.run_id"
                " AND ({old}) IS DISTINCT FROM ({new})"
                " RETURNING (xmax = 0) AS is_new)"
                " SELECT count(*) FILTER (WHERE is_new), count(*) FILTER (WHERE NOT is_new)"
                " FROM loaded"
            ).format(
                table=table, names=names, stage=staged_table,
                key=sql.SQL(", ").join(map(sql.Identifier, key)),
                sets=sql.SQL(", ").join(
                    sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in data),
                old=sql.SQL(", ").join(
                    sql.SQL("t.{}").format(sql.Identifier(c)) for c in changing),
                new=sql.SQL(", ").join(
                    sql.SQL("EXCLUDED.{}").format(sql.Identifier(c)) for c in changing),
            )).fetchone()
            after = count("SELECT count(*) FROM {}", table)

            load = {
                "mode": "initial" if before == 0 else "incremental",
                "staged": staged, "inserted": inserted, "updated": updated,
                "unchanged": staged - inserted - updated - older, "older": older,
                "removed": removed, "silver_rows_before": before,
                "silver_rows_after": after,
            }
            if load["unchanged"] < 0 or after != before + inserted - removed:
                raise LayoutError(f"Silver load of {self.run_id} does not add up: {load}")
            conn.execute(sql.SQL(
                "INSERT INTO {} (dataset, run_id, {}) VALUES ({}, {}, {})"
            ).format(
                LOAD_LOG, sql.SQL(", ").join(map(sql.Identifier, load)),
                sql.Literal(self.dataset), sql.Literal(self.run_id),
                sql.SQL(", ").join(map(sql.Literal, load.values())),
            ))
        return load

    def save_merge_result(self, summary: dict[str, Any]) -> None:
        with db.transaction() as conn:
            db.delete(conn, MERGE_RESULT, self.key)
            conn.execute(
                "INSERT INTO dq.merge_result "
                "(dataset, run_id, merged_at, published_by, result) "
                "VALUES (%s, %s, %s, %s, %s)",
                (self.dataset, self.run_id, summary["merged_at"],
                 summary.get("published_by"), _json(summary)),
            )

    def load_final(self) -> pd.DataFrame:
        return self._load_records("silver")

    def load_merge_result(self) -> dict[str, Any] | None:
        with db.transaction() as conn:
            row = conn.execute(
                "SELECT result FROM dq.merge_result WHERE dataset = %s AND run_id = %s",
                (self.dataset, self.run_id),
            ).fetchone()
        return row[0] if row else None

    def ledger_stamp(self) -> int:
        """Changes whenever the ledger changes (for caches)."""

        with db.transaction() as conn:
            return conn.execute(
                "SELECT coalesce(max(history_id), 0) FROM dq.quarantine_history "
                "WHERE dataset = %s AND run_id = %s",
                (self.dataset, self.run_id),
            ).fetchone()[0]


def list_runs(dataset: str) -> list[str]:
    """Validated runs of the dataset, oldest first."""

    with db.transaction() as conn:
        rows = conn.execute(
            "SELECT run_id FROM dq.round_result WHERE dataset = %s AND round = 1 "
            "ORDER BY run_id",
            (dataset,),
        ).fetchall()
    return [row[0] for row in rows]


def latest_run(dataset: str) -> str:
    """Most recent validated run."""

    runs = list_runs(dataset)
    if not runs:
        raise LayoutError(f"No validated runs for {dataset}")
    return runs[-1]


# ============================================================
# Files people exchange (correction templates) and the ledger
# ============================================================

def read_text_csv(path: Path) -> pd.DataFrame:
    """Read every column as text; empty cells become null."""

    frame = pd.read_csv(
        path, dtype="string", keep_default_na=False, encoding="utf-8-sig",
    )
    return frame.mask(frame.eq(""))


def write_csv(frame: pd.DataFrame, path: Path, excel: bool = False) -> None:
    """Write a CSV; excel=True adds a BOM so Excel reads UTF-8 correctly."""

    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig" if excel else "utf-8")


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_status(layout: Layout) -> pd.DataFrame:
    with db.transaction() as conn:
        if not layout.exists():
            raise LayoutError(
                f"Run {layout.run_id} has no quarantine ledger; validate it first."
            )
        status = db.select(conn, STATUS, STATUS_COLUMNS, layout.key,
                           order_by=["record_id"])
    status["round"] = status["round"].astype(int)
    status["attempts"] = status["attempts"].astype(int)
    return status


def save_status(layout: Layout, status: pd.DataFrame) -> None:
    """Replace the ledger and add the rows that changed to its history."""

    status = status[STATUS_COLUMNS]
    with db.transaction() as conn:
        previous = db.select(conn, STATUS, STATUS_COLUMNS, layout.key)
        key = ["record_id", "status", "round", "attempts"]
        merged = status.astype({"round": int, "attempts": int}).merge(
            previous[key].astype({"round": int, "attempts": int}),
            on=key, how="left", indicator=True,
        )
        changed = status[merged["_merge"].eq("left_only").to_numpy()]

        db.delete(conn, STATUS, layout.key)
        db.write_frame(conn, STATUS, status, fixed=layout.key)
        db.write_frame(
            conn, HISTORY,
            changed.rename(columns={"updated_at": "changed_at"})[HISTORY_COLUMNS],
            fixed=layout.key,
        )


def load_history(layout: Layout) -> pd.DataFrame:
    """Status changes in order."""

    with db.transaction() as conn:
        history = db.select(conn, HISTORY, HISTORY_COLUMNS, layout.key,
                            order_by=["history_id"])
    history["round"] = history["round"].astype(int)
    history["attempts"] = history["attempts"].astype(int)
    return history


def current_round(status: pd.DataFrame) -> int | None:
    """Round holding the open records (None when nothing is open)."""

    open_rounds = status.loc[status["status"].eq(OPEN), "round"]
    return int(open_rounds.max()) if len(open_rounds) else None


def data_columns(frame: pd.DataFrame) -> list[str]:
    return [c for c in frame.columns if c not in DQ_COLUMNS]
