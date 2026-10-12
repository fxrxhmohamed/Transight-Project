"""Bronze layer in PostgreSQL (schema: sql/01_bronze.sql).

    bronze.ingestion    one row per ingestion: source, SHA-256 of the
                        source file, row count, columns, record id format,
                        profile
    bronze.<dataset>    one table per dataset, laid out like the source:
                        one row per source row, one text column per source
                        column (values exactly as read), plus ingestion_id,
                        row_number and record_id

The tables are append-only (enforced by triggers): rows are never updated
or deleted, and an ingestion only gets its profile filled in later.

Connection: METAFLOW_DB_URL (a libpq URL) if set, otherwise POSTGRES_HOST,
POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER and POSTGRES_PASSWORD, with the
defaults of docker-compose.yml.

Used by ingestion (write), the profiler and the validation engine (read).
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb


MIN_ROW_DIGITS = 6

# Columns every dataset table has before the source columns.
RECORD_FIELDS = ("ingestion_id", "row_number", "record_id")
RESERVED_TABLES = {"ingestion", "ingestion_summary", "raw_record"}
MAX_IDENTIFIER_BYTES = 63   # PostgreSQL truncates longer names

INGESTION_FIELDS = (
    "ingestion_id", "dataset", "source_path", "source_file", "sha256",
    "size_bytes", "encoding", "delimiter", "row_count", "columns",
    "table_name", "record_id_prefix", "record_id_width", "ingested_at", "profile",
)


class BronzeError(LookupError):
    """An ingestion that does not exist or does not match its rows."""


def connect() -> psycopg.Connection:
    url = os.environ.get("METAFLOW_DB_URL")
    if url:
        return psycopg.connect(url)
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "metaflow"),
        user=os.environ.get("POSTGRES_USER", "metaflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "change_me_local_only"),
    )


# ============================================================
# Write
# ============================================================

def table_name_of(dataset: str) -> str:
    """bronze table of a dataset: its name in lower case, [a-z0-9_] only."""

    name = re.sub(r"[^a-z0-9_]+", "_", dataset.lower()).strip("_") or "dataset"
    if name[0].isdigit():
        name = f"ds_{name}"
    name = name[:MAX_IDENTIFIER_BYTES]
    if name in RESERVED_TABLES:
        name = f"{name}_data"
    return name


def check_columns(columns: list[str]) -> None:
    """Source columns must make distinct, unreserved PostgreSQL column names."""

    if len(set(columns)) != len(columns):
        repeated = sorted({c for c in columns if columns.count(c) > 1})
        raise ValueError(f"Repeated column names in the header: {repeated}")
    if any(not c for c in columns):
        raise ValueError("The header has an empty column name.")
    reserved = sorted(set(columns) & set(RECORD_FIELDS))
    if reserved:
        raise ValueError(
            f"Header uses names bronze reserves for its own columns: {reserved}"
        )
    long = [c for c in columns if len(c.encode("utf-8")) > MAX_IDENTIFIER_BYTES]
    if long:
        raise ValueError(
            f"Column names longer than {MAX_IDENTIFIER_BYTES} bytes "
            f"(PostgreSQL's limit): {long}"
        )


def ensure_table(conn: psycopg.Connection, table: str, columns: list[str]) -> None:
    """Create bronze.<table> (append-only), or add the columns it lacks.

    Rows of earlier ingestions have NULL in columns added later.
    """

    target = sql.Identifier("bronze", table)
    conn.execute(sql.SQL(
        "CREATE TABLE IF NOT EXISTS {} ("
        "ingestion_id text NOT NULL REFERENCES bronze.ingestion, "
        "row_number integer NOT NULL CHECK (row_number > 0), "
        "record_id text NOT NULL UNIQUE, "
        "PRIMARY KEY (ingestion_id, row_number))"
    ).format(target))

    for column in columns:
        conn.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} text")
                     .format(target, sql.Identifier(column)))

    for suffix, timing in (
        ("append_only", "BEFORE UPDATE OR DELETE ON {} FOR EACH ROW"),
        ("no_truncate", "BEFORE TRUNCATE ON {} FOR EACH STATEMENT"),
    ):
        trigger = f"{table[:MAX_IDENTIFIER_BYTES - 12]}_{suffix}"
        conn.execute(sql.SQL(
            "CREATE OR REPLACE TRIGGER {} " + timing
            + " EXECUTE FUNCTION bronze.refuse_change()"
        ).format(sql.Identifier(trigger), target))


def next_ingestion_id(conn: psycopg.Connection, today: str) -> str:
    """ING_<today>_NNN, one above the highest number for today.

    Call inside the transaction that inserts the ingestion, after locking
    bronze.ingestion, so two runs never get the same id.
    """

    row = conn.execute(
        "SELECT max(ingestion_id) FROM bronze.ingestion WHERE ingestion_id LIKE %s",
        (f"ING_{today}_%",),
    ).fetchone()
    number = int(row[0][-3:]) if row and row[0] else 0
    return f"ING_{today}_{number + 1:03d}"


def store(
    conn: psycopg.Connection,
    source: Path,
    source_path: str,
    dataset: str,
    sha256: str,
    columns: list[str],
    row_count: int,
    encoding: str,
    delimiter: str,
    started: datetime,
) -> dict[str, Any]:
    """Load every row of the source into bronze.<dataset> in one transaction."""

    check_columns(columns)
    table = table_name_of(dataset)
    width = max(MIN_ROW_DIGITS, len(str(row_count)))

    with conn.transaction():
        conn.execute("LOCK TABLE bronze.ingestion IN SHARE ROW EXCLUSIVE MODE")
        ingestion_id = next_ingestion_id(conn, started.strftime("%Y%m%d"))
        prefix = f"{ingestion_id}_ROW_"

        conn.execute(
            """
            INSERT INTO bronze.ingestion (
                ingestion_id, dataset, source_path, source_file, sha256,
                size_bytes, encoding, delimiter, row_count, columns,
                table_name, record_id_prefix, record_id_width, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (ingestion_id, dataset, source_path, source.name, sha256,
             source.stat().st_size, encoding, delimiter, row_count, columns,
             table, prefix, width, started),
        )
        ensure_table(conn, table, columns)
        target = sql.Identifier("bronze", table)

        with source.open(encoding=encoding, newline="") as handle, \
                conn.cursor().copy(sql.SQL("COPY {} ({}) FROM STDIN").format(
                    target,
                    sql.SQL(", ").join(
                        map(sql.Identifier, (*RECORD_FIELDS, *columns))),
                )) as copy:
            reader = csv.reader(handle, delimiter=delimiter)
            next(reader, None)
            for number, row in enumerate(reader, start=1):
                if len(row) > len(columns):
                    raise ValueError(
                        f"Data row {number} has {len(row)} fields; the header "
                        f"has {len(columns)}."
                    )
                # Short rows: the missing trailing fields are NULL.
                row += [None] * (len(columns) - len(row))
                copy.write_row((
                    ingestion_id, number, f"{prefix}{number:0{width}d}", *row,
                ))

        stored = conn.execute(
            sql.SQL("SELECT count(*) FROM {} WHERE ingestion_id = %s")
            .format(target),
            (ingestion_id,),
        ).fetchone()[0]
        if stored != row_count:
            raise BronzeError(
                f"Stored {stored} rows for {ingestion_id}; the source has "
                f"{row_count}."
            )

    return find_ingestion(conn, dataset, ingestion_id)


def _strict_json(value: Any) -> str:
    """JSON without NaN or Infinity, which PostgreSQL rejects."""

    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def save_profile(
    conn: psycopg.Connection, ingestion_id: str, profile: dict[str, Any],
) -> None:
    with conn.transaction():
        conn.execute(
            "UPDATE bronze.ingestion SET profile = %s WHERE ingestion_id = %s",
            (Jsonb(profile, dumps=_strict_json), ingestion_id),
        )


# ============================================================
# Read
# ============================================================

def find_ingestion(
    conn: psycopg.Connection, dataset: str, ingestion_id: str,
) -> dict[str, Any]:
    """An ingestion of the dataset; 'latest' picks the newest one.

    Returned in the shape of the former manifest.json.
    """

    query = sql.SQL(
        "SELECT {} FROM bronze.ingestion WHERE dataset = %s {} "
        "ORDER BY ingestion_id DESC LIMIT 1"
    ).format(
        sql.SQL(", ").join(map(sql.Identifier, INGESTION_FIELDS)),
        sql.SQL("") if ingestion_id == "latest"
        else sql.SQL("AND ingestion_id = %s"),
    )
    params = (dataset,) if ingestion_id == "latest" else (dataset, ingestion_id)
    row = conn.execute(query, params).fetchone()
    if row is None:
        what = "ingestions" if ingestion_id == "latest" else ingestion_id
        raise BronzeError(f"No {what} of {dataset} in bronze.ingestion")

    found = dict(zip(INGESTION_FIELDS, row))
    prefix = found.pop("record_id_prefix")
    width = found.pop("record_id_width")
    count = found["row_count"]
    found["column_count"] = len(found["columns"])
    found["ingested_at"] = found["ingested_at"].isoformat()
    found["record_id"] = {
        "prefix": prefix,
        "width": width,
        "first": f"{prefix}{1:0{width}d}" if count else None,
        "last": f"{prefix}{count:0{width}d}" if count else None,
    }
    return found


def read_frame(conn: psycopg.Connection, ingestion: dict[str, Any]):
    """Rows of an ingestion in source order, every column as text.

    Empty values come back as empty strings, as when reading the CSV.
    """

    import pandas as pd

    columns = ingestion["columns"]
    if not ingestion["table_name"]:
        raise BronzeError(
            f"{ingestion['ingestion_id']} was stored before per-dataset tables "
            "(bronze.raw_record); ingest the source again."
        )
    query = sql.SQL(
        "COPY (SELECT {} FROM {} WHERE ingestion_id = {} "
        "ORDER BY row_number) TO STDOUT WITH (FORMAT csv)"
    ).format(
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.Identifier("bronze", ingestion["table_name"]),
        sql.Literal(ingestion["ingestion_id"]),
    )

    buffer = io.BytesIO()
    with conn.cursor().copy(query) as copy:
        for chunk in copy:
            buffer.write(chunk)

    if not buffer.tell():
        return pd.DataFrame({c: pd.Series(dtype="string") for c in columns})

    buffer.seek(0)
    frame = pd.read_csv(
        buffer, header=None, names=columns, dtype="string",
        keep_default_na=False, skip_blank_lines=False, encoding="utf-8",
    )
    if len(frame) != ingestion["row_count"]:
        raise BronzeError(
            f"Read {len(frame)} rows of {ingestion['ingestion_id']}; it "
            f"records {ingestion['row_count']}."
        )
    return frame
