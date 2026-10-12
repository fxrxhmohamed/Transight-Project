"""End-to-end tests of the data-quality pipeline on the test dataset.

tests/data/banking_sample.csv has 50 rows: clean ones plus one example of
each defect, listed in tests/data/expected_defects.csv with the rule that
must catch it and the outcome. Every step runs through dq.py, as Airflow
runs it.

Every step stores its results in PostgreSQL (docker compose up -d
postgres, or the POSTGRES_* variables of .env). The tests create a
temporary database with sql/*.sql and drop it at the end; the user needs
CREATEDB.

    python -m pytest tests
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pandas as pd
import psycopg
import pytest


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "tests" / "data"
SAMPLE = DATA / "banking_sample.csv"
DATASET = "banking_transactions"
EXPECTED = pd.read_csv(DATA / "expected_defects.csv")

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "services" / "dq-service" / "src"))
from dq import load_env  # noqa: E402
from layout import Layout, load_status  # noqa: E402


def dq(*args: str | Path) -> subprocess.CompletedProcess:
    """Run one dq.py step; fail the test with its output if it fails."""

    result = subprocess.run(
        [sys.executable, str(ROOT / "dq.py"), *map(str, args)],
        cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
    )
    assert result.returncode == 0, (
        f"dq.py {' '.join(map(str, args))} failed:\n{result.stdout}\n{result.stderr}"
    )
    return result


def read(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    return frame.mask(frame.eq(""))


class Run:
    """One test run: its tables (layout) and a folder for exchanged files."""

    def __init__(self, folder: Path, run_id: str) -> None:
        self.folder = folder
        self.run_id = run_id
        self.layout = Layout(DATASET, run_id)
        self.options = ["--run", run_id]

    def record(self, row: int) -> str:
        return f"{self.run_id}_ROW_{row:06d}"

    def rows(self, defect_prefix: str) -> list[int]:
        return list(EXPECTED.loc[EXPECTED["defect"].str.startswith(defect_prefix), "row"])


def connect(dbname: str, **options) -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "metaflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "change_me_local_only"),
        dbname=dbname, **options,
    )


@pytest.fixture(scope="module")
def database() -> str:
    """A temporary bronze database for the test run (dropped afterwards)."""

    load_env(ROOT / ".env")
    os.environ.pop("METAFLOW_DB_URL", None)
    admin_db = os.environ.get("POSTGRES_DB", "metaflow")
    name = f"metaflow_test_{uuid.uuid4().hex[:8]}"

    with connect(admin_db, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with connect(name) as conn:
            for script in sorted((ROOT / "sql").glob("[0-9]*.sql")):
                conn.execute(script.read_text(encoding="utf-8"))
        os.environ["POSTGRES_DB"] = name
        yield name
    finally:
        os.environ["POSTGRES_DB"] = admin_db
        with connect(admin_db, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture(scope="module")
def run(database: str, tmp_path_factory: pytest.TempPathFactory) -> Run:
    """Ingest, profile and validate the test dataset once."""

    folder = tmp_path_factory.mktemp("dq")
    ingested = dq("ingest", "--input", SAMPLE)
    test_run = Run(folder, ingested.stdout.strip().splitlines()[-1])
    dq("profile", *test_run.options)
    dq("validate", *test_run.options)
    return test_run


# ============================================================
# Ingest and profile
# ============================================================

def test_ingest_stores_every_row_unchanged(run: Run, database: str) -> None:
    with connect(database) as conn:
        sha256, row_count, columns = conn.execute(
            "SELECT sha256, row_count, columns FROM bronze.ingestion "
            "WHERE ingestion_id = %s", (run.run_id,),
        ).fetchone()
        cursor = conn.execute(
            f"SELECT * FROM bronze.{DATASET} "
            "WHERE ingestion_id = %s ORDER BY row_number", (run.run_id,),
        )
        table_columns = [column.name for column in cursor.description]
        rows = cursor.fetchall()

    sample = pd.read_csv(SAMPLE, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    assert sha256 == hashlib.sha256(SAMPLE.read_bytes()).hexdigest()
    assert row_count == len(rows) == 50
    assert columns == list(sample.columns)
    # Laid out like the CSV: one column per source column, in order.
    assert table_columns == ["ingestion_id", "row_number", "record_id", *columns]
    assert rows[0][2] == run.record(1)
    assert [list(row[3:]) for row in rows] == sample.values.tolist()


def test_bronze_is_append_only(run: Run, database: str) -> None:
    for statement in (
        f"UPDATE bronze.{DATASET} SET record_id = '' WHERE ingestion_id = %s",
        f"DELETE FROM bronze.{DATASET} WHERE ingestion_id = %s",
        "UPDATE bronze.ingestion SET row_count = 0 WHERE ingestion_id = %s",
        "DELETE FROM bronze.ingestion WHERE ingestion_id = %s",
    ):
        with connect(database) as conn, pytest.raises(psycopg.errors.RaiseException):
            conn.execute(statement, (run.run_id,))


def test_profile_is_stored_with_the_ingestion(run: Run, database: str) -> None:
    with connect(database) as conn:
        profile = conn.execute(
            "SELECT profile FROM bronze.ingestion WHERE ingestion_id = %s",
            (run.run_id,),
        ).fetchone()[0]

    assert profile["ingestion_id"] == run.run_id
    assert profile["row_count"] == 50


# ============================================================
# Validate: every defect is caught, clean rows are left alone
# ============================================================

@pytest.fixture(scope="module")
def first_round(run: Run) -> dict[str, pd.DataFrame]:
    return {
        "valid": run.layout.load_valid(),
        "quarantine": run.layout.load_quarantine(1),
        "errors": run.layout.load_errors(1),
        "audit": run.layout.load_audit(1),
    }


def warnings_of(valid: pd.DataFrame, record: str) -> set[str]:
    found = valid.loc[valid["record_id"].eq(record), "dq_warnings"].dropna()
    return set(found.iloc[0].split(";")) if len(found) else set()


@pytest.mark.parametrize(
    "defect", EXPECTED.to_dict("records"), ids=lambda d: f"row{d['row']}-{d['defect']}",
)
def test_defect_is_caught(run: Run, first_round: dict, defect: dict) -> None:
    record = run.record(defect["row"])
    valid, audit = first_round["valid"], first_round["audit"]
    in_valid = record in set(valid["record_id"])
    in_quarantine = record in set(first_round["quarantine"]["record_id"])
    repaired = (
        audit["record_id"].eq(record) & audit["rule_id"].eq(defect["rule_id"])
        & audit["outcome"].isin(["remediated", "removed"])
    ).any()
    errors = first_round["errors"]
    failed = (errors["record_id"].eq(record) & errors["rule_id"].eq(defect["rule_id"])).any()

    outcome = defect["outcome"]
    if outcome == "repaired":
        assert in_valid and repaired, "should be repaired and accepted"
        assert not warnings_of(valid, record), "a repaired row has no warning"
    elif outcome == "removed":
        assert repaired and not in_valid and not in_quarantine, "should be dropped"
    elif outcome == "flagged":
        assert in_valid, "should be accepted"
        assert defect["rule_id"] in warnings_of(valid, record), "should carry the warning"
    elif outcome == "quarantined":
        assert in_quarantine and failed, "should be quarantined by this rule"
    else:
        pytest.fail(f"unknown outcome {outcome}")


def test_clean_rows_pass_untouched(run: Run, first_round: dict) -> None:
    defect_rows = set(EXPECTED["row"])
    clean = [run.record(n) for n in range(1, 51) if n not in defect_rows]
    valid = first_round["valid"].set_index("record_id")

    # A clean row may only be quarantined as the twin of a duplicated id,
    # or flagged when it is the majority row of a conflict.
    quarantined = set(first_round["quarantine"]["record_id"]) & set(clean)
    assert not quarantined
    touched = set(first_round["audit"]["record_id"]) & set(clean)
    assert not touched
    assert valid.loc[[r for r in clean if r in valid.index], "dq_warnings"].isna().all()


def test_counts(run: Run) -> None:
    result = run.layout.load_results()[1]["counts"]
    quarantined = int((EXPECTED["outcome"] == "quarantined").sum())
    removed = int((EXPECTED["outcome"] == "removed").sum())

    assert result["input_rows"] == 50
    assert result["duplicate_rows_removed"] == removed
    assert result["quarantined_rows"] == quarantined
    assert result["accepted_rows"] == 50 - removed - quarantined


# ============================================================
# Correct, re-validate, merge, metrics
# ============================================================

def metrics_table(database: str, table: str, run_id: str) -> pd.DataFrame:
    with connect(database) as conn:
        cursor = conn.execute(
            f"SELECT * FROM metrics.{table} WHERE run_id = %s", (run_id,),
        )
        return pd.DataFrame(cursor.fetchall(),
                            columns=[column.name for column in cursor.description])


def test_correction_flow(run: Run, first_round: dict, database: str) -> None:
    template_file = run.folder / "template.csv"
    dq("export-template", *run.options, "--output", template_file)
    template = read(template_file).fillna("")
    assert set(template["record_id"]) == set(first_round["quarantine"]["record_id"])

    credit = run.record(run.rows("Credit score")[0])
    customer = run.record(run.rows("Missing customer ID")[0])
    first_copy, second_copy = (run.record(n) for n in run.rows("Transaction ID used twice"))
    review = run.record(run.rows("Three values repaired")[0])
    kept_id = first_round["quarantine"].set_index("record_id").at[first_copy, "transaction_id"]

    def fill(record: str, column: str, value: str = "", action: str = "") -> None:
        line = template["record_id"].eq(record) & template["column"].eq(column)
        assert line.any(), f"no template line for {record} {column}"
        template.loc[line, ["new_value", "action", "reason"]] = [value, action, "test"]

    fill(credit, "credit_score", "700")
    fill(customer, "customer_id", "CUST999001")
    fill(first_copy, "transaction_id", kept_id)
    fill(second_copy, "transaction_id", action="DISCARD")
    fill(review, "", action="ACCEPT")
    template.to_csv(template_file, index=False)

    imported = dq("import-corrections", *run.options, "--file", template_file, "--user", "ci")
    assert "5 corrections accepted" in imported.stdout
    dq("revalidate", *run.options)

    status = load_status(run.layout).set_index("record_id")
    assert status.loc[[credit, customer, first_copy, review], "status"].eq("RESOLVED").all()
    assert status.at[second_copy, "status"] == "DISCARDED"
    still_open = status["status"].eq("OPEN")
    assert still_open.sum() == len(status) - 5
    assert status.loc[still_open, "round"].eq(2).all()

    # ---- Merge ----
    dq("merge", *run.options)
    clean = run.layout.load_final()
    valid = run.layout.load_valid()
    [(resolved_round, resolved)] = run.layout.load_resolved()
    assert resolved_round == 1
    assert len(clean) == len(valid) + len(resolved)
    assert not clean["record_id"].duplicated().any()
    assert clean.set_index("record_id").at[credit, "credit_score"] == "700"
    assert second_copy not in set(clean["record_id"])

    # ---- Metrics ----
    dq("metrics", *run.options)
    runs = metrics_table(database, "dq_run", run.run_id).sort_values("round")
    last = runs.iloc[-1]
    assert list(runs["round"]) == [1, 2]
    assert int(last["manually_fixed_records"]) == 3
    assert int(last["confirmed_records"]) == 1
    assert int(last["discarded"]) == 1
    assert (
        int(last["resolved"]) + int(last["discarded"]) + int(last["unresolved_records"])
        == int(last["quarantined"])
    )
    assert float(last["final_dq_score"]) > float(last["initial_dq_score"])

    corrections = metrics_table(database, "dq_correction", run.run_id)
    manual = corrections[corrections["correction_type"].eq("manual")]
    assert set(manual["outcome"]) == {"resolved", "discarded"}
    for table in ("dq_rule_result", "dq_quarantine"):
        assert len(metrics_table(database, table, run.run_id))


# ============================================================
# Correction UI and Airflow
# ============================================================

def test_correction_ui_renders(run: Run) -> None:
    testing = pytest.importorskip("streamlit.testing.v1")
    argv = sys.argv
    sys.argv = ["correction_ui.py"]
    try:
        app = testing.AppTest.from_file(
            str(ROOT / "services" / "dq-service" / "src" / "correction_ui.py"),
            default_timeout=120,
        ).run()
        assert not app.exception, [e.message for e in app.exception]
        for page in app.sidebar.radio[0].options[1:]:
            app.sidebar.radio[0].set_value(page).run()
            assert not app.exception, (page, [e.message for e in app.exception])
    finally:
        sys.argv = argv


def test_airflow_dags_only_call_known_steps() -> None:
    """Every task(task_id, step, ...) in the DAG files names a dq.py step."""

    import ast

    sys.path.insert(0, str(ROOT))
    from dq import parse_args

    dag_files = list((ROOT / "airflow" / "dags").glob("*.py"))
    assert dag_files
    steps = {
        node.args[1].value
        for path in dag_files
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "task"
        and len(node.args) > 1 and isinstance(node.args[1], ast.Constant)
    }
    assert {"ingest", "profile", "validate", "import-corrections",
            "revalidate", "merge", "metrics"} <= steps
    for step in steps:
        extra = ["--file", "x.csv"] if step == "import-corrections" else []
        assert parse_args([step, *extra]).step == step
