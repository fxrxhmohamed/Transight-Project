"""Ingestion: land a source file in the bronze layer (PostgreSQL).

Each run gets an ingestion id (ING_YYYYMMDD_NNN) and is loaded in one
transaction (see bronze.py and sql/01_bronze.sql):

    bronze.ingestion    what was ingested, from where, when, the source
                        file's SHA-256, row count, columns and how record
                        ids are formed
    bronze.<dataset>    every data row, one column per source column,
                        values exactly as read

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
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import bronze


def _default_project_root() -> Path:
    """Project root is 3 levels above this file; fall back to the cwd."""

    parents = Path(__file__).resolve().parents
    return parents[3] if len(parents) > 3 else Path.cwd()


PROJECT_ROOT = _default_project_root()

DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "banking" / "banking_transactions.csv"
DEFAULT_DATASET = "banking_transactions"

HASH_CHUNK = 1 << 20


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(HASH_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_layout(path: Path, encoding: str, delimiter: str) -> tuple[list[str], int]:
    """Header and number of data rows (quoted line breaks count once)."""

    with path.open(encoding=encoding, newline="") as handle:
        reader = csv.reader(handle, delimiter=delimiter)
        header = next(reader, [])
        rows = sum(1 for _ in reader)

    return header, rows


def relative_to_root(path: Path) -> str:
    """Path relative to the project root when possible (portable across machines)."""

    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def ingest(
    source: Path,
    dataset: str,
    encoding: str = "utf-8-sig",
    delimiter: str = ",",
) -> dict[str, Any]:
    """Load the source into bronze; returns the ingestion record."""

    if not source.is_file():
        raise FileNotFoundError(f"Input not found: {source}")

    started = datetime.now(timezone.utc)
    columns, row_count = read_layout(source, encoding, delimiter)

    with bronze.connect() as conn:
        return bronze.store(
            conn, source, relative_to_root(source), dataset,
            sha256_of(source), columns, row_count, encoding, delimiter,
            started,
        )


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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = ingest(
        args.input.resolve(), args.dataset, args.encoding, args.delimiter,
    )

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
