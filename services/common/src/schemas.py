"""Create the database schemas a step needs when they are missing.

Docker runs sql/*.sql only when the database volume is first created, so a
volume made before a schema existed gets it from here, on first use. Each
file is applied when its marker table is missing (every file is safe to
run again).
"""

from __future__ import annotations

from pathlib import Path

import psycopg

# SQL file -> a table it creates.
MARKERS = {
    "01_bronze.sql": "bronze.ingestion",
    "02_dq.sql": "dq.run",
    "03_meta_gold.sql": "meta.dataset",
}

_ready: set[str] = set()


def _sql_folder() -> Path | None:
    here = Path(__file__).resolve()
    for folder in (here.parent, here.parents[3] / "sql" if len(here.parents) > 3 else None):
        if folder and (folder / "02_dq.sql").is_file():
            return folder
    return None


def ensure(conn: psycopg.Connection, *files: str) -> None:
    """Apply the given sql/ files whose marker table does not exist."""

    for name in files:
        if name in _ready:
            continue
        missing = conn.execute("SELECT to_regclass(%s)", (MARKERS[name],)).fetchone()[0] is None
        if missing:
            folder = _sql_folder()
            if folder is None:
                raise RuntimeError(
                    f"{MARKERS[name]} is missing and sql/{name} was not found; "
                    f"run psql -f sql/{name} on the database."
                )
            conn.execute((folder / name).read_text(encoding="utf-8"))
            conn.commit()
        _ready.add(name)
