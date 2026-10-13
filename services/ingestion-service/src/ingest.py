"""Ingestion: land a source file in the bronze layer (PostgreSQL).

Spark reads the file (in parallel, every value as text, exactly as
written) into a staging table; one transaction then gives the run an
ingestion id (ING_YYYYMMDD_NNN) and moves the rows into bronze (see
bronze.py and sql/01_bronze.sql), so an ingestion is stored completely or
not at all:

    bronze.ingestion    what was ingested, from where, when, the source
                        file's SHA-256, row count, columns and how record
                        ids are formed
    bronze.<dataset>    every data row, one column per source column,
                        values exactly as read

A file whose content (SHA-256) is already in bronze for the dataset is
not loaded again: ingest stops with exit code 99 (Airflow marks the task,
and so the rest of the run, skipped) unless --force is given. A new file
holding new and changed records is an incremental load: merge.py upserts
it into silver by primary key.

Bronze is append-only. Later steps read an ingestion from it and
identify every row by its record id:

    <ingestion_id>_ROW_<data row number, zero-padded>
    e.g. ING_20261011_001_ROW_0000001 is the first data row

so a record can be traced from bronze to quarantine, correction and the
final dataset.

Usage
-----
    # Banking defaults
    python src/ingest.py

    # Another file or dataset
    python src/ingest.py --input landing/sales.csv --dataset sales

    # The same file again, on purpose
    python src/ingest.py --force
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Shared code: the bronze layer client and the Spark helpers.
sys.path.append(str(Path(__file__).resolve().parents[2] / "common" / "src"))
import bronze  # noqa: E402
import spark_io  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402


def _default_project_root() -> Path:
    """Project root is 3 levels above this file; fall back to the cwd."""

    parents = Path(__file__).resolve().parents
    return parents[3] if len(parents) > 3 else Path.cwd()


PROJECT_ROOT = _default_project_root()

DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "banking" / "banking_transactions.csv"
DEFAULT_DATASET = "banking_transactions"

HASH_CHUNK = 1 << 20
# Extra columns read past the header: a row that fills one has more fields
# than the header and stops the ingestion.
OVERFLOW_COLUMNS = 3
# Exit code of a file already ingested (Airflow's BashOperator: skipped).
ALREADY_INGESTED_EXIT = 99


class AlreadyIngested(Exception):
    def __init__(self, ingestion_id: str) -> None:
        super().__init__(ingestion_id)
        self.ingestion_id = ingestion_id


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(HASH_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_header(path: Path, encoding: str, delimiter: str) -> list[str]:
    with path.open(encoding=encoding, newline="") as handle:
        return next(csv.reader(handle, delimiter=delimiter), [])


def relative_to_root(path: Path) -> str:
    """Path relative to the project root when possible (portable across machines)."""

    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def read_rows(source: Path, columns: list[str], encoding: str, delimiter: str):
    """Every data row in file order with its row number (1, 2, ...).

    Values are text exactly as written. Empty fields are stored as empty
    text; so are the missing fields of a row shorter than the header (Spark
    reads both as null and cannot tell them apart).
    """

    overflow = [f"__overflow{i}" for i in range(OVERFLOW_COLUMNS)]
    schema = StructType([StructField(c, StringType()) for c in [*columns, *overflow]])
    df = (
        spark_io.session().read.schema(schema)
        .option("header", True).option("enforceSchema", True)
        .option("encoding", encoding.replace("-sig", "")).option("sep", delimiter)
        .option("multiLine", True).option("quote", '"').option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(str(source))
    )
    order = Window.orderBy(F.monotonically_increasing_id())
    df = df.select(F.row_number().over(order).alias("row_number"), "*")

    long_rows = df.filter(" OR ".join(f"`{c}` IS NOT NULL" for c in overflow))
    first = long_rows.select("row_number").limit(1).collect()
    if first:
        raise ValueError(
            f"Data row {first[0]['row_number']} has more fields than the header "
            f"({len(columns)})."
        )
    return df.select("row_number", *[
        F.coalesce(F.col(f"`{c}`"), F.lit("")).alias(c) for c in columns
    ])


def ingested_as(dataset: str, sha256: str) -> str | None:
    """The first ingestion of the dataset with this file content, if any."""

    with bronze.connect() as conn:
        row = conn.execute(
            "SELECT ingestion_id FROM bronze.ingestion "
            "WHERE dataset = %s AND sha256 = %s ORDER BY ingestion_id LIMIT 1",
            (dataset, sha256),
        ).fetchone()
    return row[0] if row else None


def ingest(
    source: Path,
    dataset: str,
    encoding: str = "utf-8-sig",
    delimiter: str = ",",
    force: bool = False,
) -> dict[str, Any]:
    """Load the source into bronze; returns the ingestion record.

    Raises AlreadyIngested when the same content is already in bronze
    (unless force).
    """

    if not source.is_file():
        raise FileNotFoundError(f"Input not found: {source}")

    sha256 = sha256_of(source)
    previous = None if force else ingested_as(dataset, sha256)
    if previous:
        raise AlreadyIngested(previous)

    started = datetime.now(timezone.utc)
    columns = read_header(source, encoding, delimiter)
    bronze.check_columns(columns)

    rows = read_rows(source, columns, encoding, delimiter).cache()
    row_count = rows.count()

    stage = f"_stage_{uuid.uuid4().hex[:12]}"
    with bronze.connect() as conn:
        try:
            spark_io.write(rows, bronze.create_stage(conn, stage, columns))
            return bronze.store_staged(
                conn, stage, source, relative_to_root(source), dataset,
                sha256, columns, row_count, encoding, delimiter, started,
            )
        finally:
            bronze.drop_stage(conn, stage)
            rows.unpersist()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load a CSV file into the bronze layer (PostgreSQL) "
        "with an ingestion id.",
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                        help="Source CSV file.")
    parser.add_argument("--dataset", default=DEFAULT_DATASET,
                        help="Dataset name.")
    parser.add_argument("--encoding", default="utf-8-sig")
    parser.add_argument("--delimiter", default=",")
    parser.add_argument("--force", action="store_true",
                        help="Load the file even when the same content is "
                        "already in bronze.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        manifest = ingest(
            args.input.resolve(), args.dataset, args.encoding, args.delimiter,
            args.force,
        )
    except AlreadyIngested as error:
        print(f"{args.input} is already in bronze as {error.ingestion_id} "
              "(same SHA-256); nothing loaded. Use --force to load it again.")
        return ALREADY_INGESTED_EXIT
    finally:
        spark_io.stop()

    print(f"Ingested {manifest['source_path']}")
    print(f"  ingestion id  {manifest['ingestion_id']}")
    print(f"  stored in     bronze.{manifest['table_name']} (append-only)")
    print(f"  rows          {manifest['row_count']}")
    print(f"  sha256        {manifest['sha256']}")
    print(f"  record ids    {manifest['record_id']['first']} .. "
          f"{manifest['record_id']['last']}")

    # Last line: the id alone, for scripts and Airflow (XCom).
    print(manifest["ingestion_id"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
