"""PostgreSQL access for the data-quality outputs (sql/02_dq.sql).

Connection settings are the bronze layer's (bronze.connect: METAFLOW_DB_URL
or the POSTGRES_* variables). Frames go in with COPY and come back with
every column as text and empty values as null, as the CSV files did;
timestamps come back in ISO form (2026-10-11T18:18:17+00:00).

    with transaction() as conn:
        ...

gives one connection and transaction to everything inside it; nested
calls join the outer one, so a step's writes are committed together or
not at all.
"""

from __future__ import annotations

import io
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Iterator

import pandas as pd
import psycopg
from psycopg import sql

# The bronze layer client lives with the ingestion service (the Docker
# image copies it next to this file).
sys.path.append(str(Path(__file__).resolve().parents[2] / "ingestion-service" / "src"))
import bronze  # noqa: E402


SCHEMA_FILE = "02_dq.sql"
CHUNK_ROWS = 100_000

# Read back in the form layout.now_utc() writes.
TIMESTAMP_COLUMNS = {
    "updated_at", "changed_at", "corrected_at", "rejected_at", "merged_at",
    "validated_at",
}
ISO_TIMESTAMP = 'YYYY-MM-DD"T"HH24:MI:SS"+00:00"'

# Columns the record tables add in front of the data columns.
RECORD_KEYS = {
    "valid": ("run_id",),
    "quarantine": ("run_id", "round"),
    "resolved": ("run_id", "round"),
    "silver": ("run_id",),
}
KEY_TYPES = {"run_id": "text", "round": "integer"}


class LayoutError(ValueError):
    """A run or round that does not exist, or a database that is not set up."""


_active: ContextVar[psycopg.Connection | None] = ContextVar("dq_connection", default=None)
_schema_ready = False


@contextmanager
def transaction() -> Iterator[psycopg.Connection]:
    conn = _active.get()
    if conn is not None:
        yield conn
        return

    # Commits when the block ends normally, rolls back on an exception.
    with bronze.connect() as conn:
        ensure_schema(conn)
        token = _active.set(conn)
        try:
            yield conn
        finally:
            _active.reset(token)


def _schema_file() -> Path | None:
    here = Path(__file__).resolve()
    candidates = [here.parent / SCHEMA_FILE]
    if len(here.parents) > 3:
        candidates.append(here.parents[3] / "sql" / SCHEMA_FILE)
    return next((path for path in candidates if path.is_file()), None)


def ensure_schema(conn: psycopg.Connection) -> None:
    """Apply sql/02_dq.sql when the dq tables are missing (database volumes
    created before them; Docker only runs sql/ on an empty volume)."""

    global _schema_ready
    if _schema_ready:
        return
    if conn.execute("SELECT to_regclass('dq.run')").fetchone()[0] is None:
        path = _schema_file()
        if path is None:
            raise LayoutError(
                "The dq tables are missing and sql/02_dq.sql was not found; "
                "run psql -f sql/02_dq.sql on the database."
            )
        conn.execute(path.read_text(encoding="utf-8"))
        conn.commit()
    _schema_ready = True


# ============================================================
# Frames in and out
# ============================================================

def select_list(columns: list[str]) -> sql.Composable:
    """Columns to select, timestamps formatted as ISO text."""

    def item(column: str) -> sql.Composable:
        if column in TIMESTAMP_COLUMNS:
            return sql.SQL("to_char({} AT TIME ZONE 'UTC', {}) AS {}").format(
                sql.Identifier(column), sql.Literal(ISO_TIMESTAMP),
                sql.Identifier(column),
            )
        return sql.Identifier(column)

    return sql.SQL(", ").join(map(item, columns))


def read_frame(
    conn: psycopg.Connection, query: sql.Composable, columns: list[str],
) -> pd.DataFrame:
    """Rows of a query (selecting exactly columns), every column as text."""

    buffer = io.BytesIO()
    statement = sql.SQL("COPY ({}) TO STDOUT WITH (FORMAT csv)").format(query)
    with conn.cursor().copy(statement) as copy:
        for chunk in copy:
            buffer.write(chunk)

    if not buffer.tell():
        return pd.DataFrame({c: pd.Series(dtype="string") for c in columns})

    buffer.seek(0)
    frame = pd.read_csv(
        buffer, header=None, names=columns, dtype="string",
        keep_default_na=False, encoding="utf-8",
    )
    return frame.mask(frame.eq(""))


def select(
    conn: psycopg.Connection,
    table: sql.Identifier,
    columns: list[str],
    where: dict[str, object],
    order_by: list[str] | None = None,
    extra: sql.Composable | None = None,
) -> pd.DataFrame:
    """SELECT columns FROM table WHERE key = value AND ... [extra] ORDER BY."""

    conditions = [
        sql.SQL("{} = {}").format(sql.Identifier(key), sql.Literal(value))
        for key, value in where.items()
    ]
    if extra is not None:
        conditions.append(extra)
    query = sql.SQL("SELECT {} FROM {} WHERE {}").format(
        select_list(columns), table, sql.SQL(" AND ").join(conditions),
    )
    if order_by:
        query += sql.SQL(" ORDER BY {}").format(
            sql.SQL(", ").join(map(sql.Identifier, order_by))
        )
    return read_frame(conn, query, columns)


def write_frame(
    conn: psycopg.Connection,
    table: sql.Identifier,
    frame: pd.DataFrame,
    fixed: dict[str, object] | None = None,
) -> None:
    """COPY a frame into table, matching columns by name.

    fixed adds columns with one value for every row (e.g. dataset, run_id).
    Null and empty values are stored as NULL.
    """

    if not len(frame):
        return
    if fixed:
        frame = frame.assign(**fixed)

    statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
        table, sql.SQL(", ").join(map(sql.Identifier, frame.columns)),
    )
    with conn.cursor().copy(statement) as copy:
        for start in range(0, len(frame), CHUNK_ROWS):
            copy.write(frame.iloc[start:start + CHUNK_ROWS].to_csv(
                index=False, header=False, lineterminator="\n",
            ))


def delete(
    conn: psycopg.Connection, table: sql.Identifier, where: dict[str, object],
) -> None:
    conn.execute(sql.SQL("DELETE FROM {} WHERE {}").format(
        table,
        sql.SQL(" AND ").join(
            sql.SQL("{} = {}").format(sql.Identifier(key), sql.Literal(value))
            for key, value in where.items()
        ),
    ))


# ============================================================
# Record tables (one per dataset and kind)
# ============================================================

def record_table_name(dataset: str, kind: str) -> tuple[str, str]:
    """(schema, table): dq.<dataset>_valid / _quarantine / _resolved, or
    silver.<dataset>."""

    if kind == "silver":
        return "silver", bronze.table_name_of(dataset)
    suffix = f"_{kind}"
    base = bronze.table_name_of(dataset)[:bronze.MAX_IDENTIFIER_BYTES - len(suffix)]
    return "dq", base + suffix


def record_table(dataset: str, kind: str) -> sql.Identifier:
    return sql.Identifier(*record_table_name(dataset, kind))


def table_columns(conn: psycopg.Connection, schema: str, name: str) -> list[str]:
    """Columns of a table in order; empty when it does not exist."""

    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, name),
    ).fetchall()
    return [row[0] for row in rows]


def ensure_record_table(
    conn: psycopg.Connection, dataset: str, kind: str, columns: list[str],
) -> sql.Identifier:
    """Create the record table, or add the text columns it lacks."""

    keys = RECORD_KEYS[kind]
    clash = sorted(set(columns) & {*keys, "record_id"})
    if clash:
        raise LayoutError(f"Data columns use names the dq tables reserve: {clash}")

    table = record_table(dataset, kind)
    existing = table_columns(conn, *record_table_name(dataset, kind))
    if not existing:
        conn.execute(sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ({}, record_id text NOT NULL, "
            "PRIMARY KEY ({}, record_id))"
        ).format(
            table,
            sql.SQL(", ").join(
                sql.SQL("{} {} NOT NULL").format(sql.Identifier(k), sql.SQL(KEY_TYPES[k]))
                for k in keys
            ),
            sql.SQL(", ").join(map(sql.Identifier, keys)),
        ))
        existing = [*keys, "record_id"]

    for column in columns:
        if column not in existing:
            conn.execute(sql.SQL("ALTER TABLE {} ADD COLUMN {} text").format(
                table, sql.Identifier(column),
            ))
    return table
