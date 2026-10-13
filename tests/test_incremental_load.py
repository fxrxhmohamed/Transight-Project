"""Initial and incremental loads into silver, end to end.

Two files cut from tests/data/banking_sample.csv:

    initial      data rows 1-20 (all clean)
    incremental  data rows 11-30: rows 11-13 with a new transaction time
                 (changed), rows 14-20 as before (unchanged), rows 21-30 new
                 (rows 21-29 are repaired automatically, row 30 is clean)

and the checks that no record is lost, duplicated or overwritten by an
older run: the load counts, the keys in silver, lineage back to bronze, a
file loaded twice and runs merged again. Runs like tests/test_pipeline.py
(own temporary database):

    docker compose run --rm pipeline python -m pytest tests/test_incremental_load.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from test_pipeline import DATASET, ROOT, SAMPLE, connect, database, dq  # noqa: F401

CHANGED_TIME = "23:59:59"
CHANGED_ROWS = [11, 12, 13]


def write_files(folder: Path) -> tuple[Path, Path, list[str]]:
    sample = pd.read_csv(SAMPLE, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    initial = folder / "load_initial.csv"
    sample.iloc[0:20].to_csv(initial, index=False)

    incremental = sample.iloc[10:30].copy()
    changed = [r - 1 for r in CHANGED_ROWS]
    incremental.loc[changed, "transaction_time"] = CHANGED_TIME
    path = folder / "load_incremental.csv"
    incremental.to_csv(path, index=False)
    return initial, path, list(sample.loc[changed, "transaction_id"])


def load(file: Path) -> str:
    """Ingest, profile, validate and merge a file; returns its run id."""

    run_id = dq("ingest", "--input", file).stdout.strip().splitlines()[-1]
    for step in ("profile", "validate", "merge"):
        dq(step, "--run", run_id)
    return run_id


def last_load(run_id: str) -> dict:
    with connect(database_name()) as conn:
        row = conn.execute(
            "SELECT mode, staged, inserted, updated, unchanged, older, removed, "
            "silver_rows_before, silver_rows_after FROM dq.load_log "
            "WHERE run_id = %s ORDER BY load_id DESC LIMIT 1", (run_id,),
        ).fetchone()
    keys = ["mode", "staged", "inserted", "updated", "unchanged", "older", "removed",
            "before", "after"]
    return dict(zip(keys, row))


def database_name() -> str:
    return os.environ["POSTGRES_DB"]


def scalar(query: str, *params) -> object:
    with connect(database_name()) as conn:
        return conn.execute(query, params or None).fetchone()[0]


def check_silver_consistent() -> None:
    """No duplicate or missing keys, every row traceable to bronze, the load
    log adds up."""

    table = f"silver.{DATASET}"
    assert scalar(f"SELECT count(*) - count(DISTINCT transaction_id) FROM {table}") == 0
    assert scalar(f"SELECT count(*) FROM {table} WHERE transaction_id IS NULL") == 0
    assert scalar(f"SELECT count(*) - count(DISTINCT record_id) FROM {table}") == 0
    assert scalar(
        f"SELECT count(*) FROM {table} s LEFT JOIN bronze.{DATASET} b "
        "USING (record_id) WHERE b.record_id IS NULL") == 0
    # Silver's version of a key comes from the bronze row it names.
    assert scalar(
        f"SELECT count(*) FROM {table} s JOIN bronze.{DATASET} b USING (record_id) "
        "WHERE b.transaction_id IS DISTINCT FROM s.transaction_id "
        "OR b.ingestion_id <> s.run_id") == 0
    assert scalar(
        "SELECT count(*) FROM dq.load_log WHERE "
        "staged <> inserted + updated + unchanged + older "
        "OR silver_rows_after <> silver_rows_before + inserted - removed") == 0


@pytest.fixture(scope="module")
def loads(database: str, tmp_path_factory: pytest.TempPathFactory) -> dict:  # noqa: F811
    folder = tmp_path_factory.mktemp("loads")
    initial, incremental, changed_ids = write_files(folder)
    first = load(initial)
    first_load = last_load(first)
    check_silver_consistent()
    second = load(incremental)
    return {"initial_file": initial, "incremental_file": incremental,
            "first": first, "second": second, "changed_ids": changed_ids,
            "first_load": first_load, "second_load": last_load(second)}


def test_initial_load(loads: dict) -> None:
    load = loads["first_load"]
    assert load["mode"] == "initial"
    assert (load["before"], load["staged"], load["inserted"], load["after"]) == (0, 20, 20, 20)
    assert (load["updated"], load["unchanged"], load["older"], load["removed"]) == (0, 0, 0, 0)


def test_incremental_load(loads: dict) -> None:
    load = loads["second_load"]
    assert load["mode"] == "incremental"
    assert load["staged"] == 20
    assert (load["inserted"], load["updated"], load["unchanged"], load["older"]) == (10, 3, 7, 0)
    assert (load["before"], load["after"]) == (20, 30)
    check_silver_consistent()

    with connect(database_name()) as conn:
        rows = conn.execute(
            f"SELECT transaction_id, run_id, transaction_time::text FROM silver.{DATASET}",
        ).fetchall()
    by_key = {key: (run_id, time) for key, run_id, time in rows}
    for key in loads["changed_ids"]:
        assert by_key[key] == (loads["second"], CHANGED_TIME)
    # Unchanged keys keep the row (and lineage) of the run that loaded them.
    assert sum(run_id == loads["first"] for run_id, _ in by_key.values()) == 17


def test_same_file_is_not_loaded_twice(loads: dict) -> None:
    ingestions = scalar("SELECT count(*) FROM bronze.ingestion")
    result = subprocess.run(
        [sys.executable, str(ROOT / "dq.py"), "ingest", "--input",
         str(loads["incremental_file"])],
        cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
    )
    assert result.returncode == 99, result.stdout + result.stderr
    assert loads["second"] in result.stdout
    assert scalar("SELECT count(*) FROM bronze.ingestion") == ingestions


def test_merging_again_changes_nothing(loads: dict) -> None:
    dq("merge", "--run", loads["second"])
    load = last_load(loads["second"])
    assert (load["inserted"], load["updated"], load["removed"]) == (0, 0, 0)
    assert load["unchanged"] == 20 and load["after"] == 30


def test_older_run_does_not_overwrite_newer(loads: dict) -> None:
    dq("merge", "--run", loads["first"])
    load = last_load(loads["first"])
    assert (load["inserted"], load["updated"], load["removed"]) == (0, 0, 0)
    assert (load["older"], load["unchanged"]) == (3, 17)
    for key in loads["changed_ids"]:
        assert scalar(f"SELECT run_id FROM silver.{DATASET} WHERE transaction_id = %s",
                      key) == loads["second"]
    check_silver_consistent()


def test_gold_matches_silver(loads: dict) -> None:
    dq("metadata")
    dq("gold")
    fact = scalar("SELECT table_name FROM information_schema.tables "
                  "WHERE table_schema = 'gold' AND table_name LIKE 'fact\\_%'")
    assert scalar(f'SELECT count(*) FROM gold."{fact}"') == \
        scalar(f"SELECT count(*) FROM silver.{DATASET}") == 30
