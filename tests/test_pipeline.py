"""End-to-end tests of the data-quality pipeline on the test dataset.

tests/data/banking_sample.csv has 50 rows: clean ones plus one example of
each defect, listed in tests/data/expected_defects.csv with the rule that
must catch it and the outcome. Every step runs through dq.py, as Airflow
runs it.

The steps run on Spark (Java needed) and store their results in
PostgreSQL. The tests create a temporary database with sql/*.sql and drop
it at the end; the user needs CREATEDB. Run them in the pipeline
container:

    docker compose run --rm pipeline python -m pytest tests
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
            f"SELECT * FROM semantic.{table} WHERE run_id = %s", (run_id,),
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
# Silver types, metadata engine, gold (AutoETL)
# ============================================================

def query(database: str, sql: str, *params) -> list[tuple]:
    with connect(database) as conn:
        return conn.execute(sql, params).fetchall()


def test_silver_is_typed(run: Run, database: str) -> None:
    types = dict(query(
        database,
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'silver' AND table_name = %s", DATASET,
    ))
    assert types["credit_score"] == "bigint"
    assert types["transaction_amount"] == "numeric"
    assert types["transaction_date"] == "date"
    assert types["transaction_time"] == "time without time zone"
    assert types["login_success"] == "boolean"
    assert types["customer_id"] == "text"


def test_metadata_and_gold(run: Run, database: str) -> None:
    dq("metadata")
    dq("gold")

    [(grain, records)] = query(
        database, "SELECT grain_entity, record_count FROM meta.dataset WHERE dataset = %s",
        DATASET)
    assert grain == "transaction"
    entities = dict(query(database, "SELECT entity, distinct_keys FROM meta.entity "
                                    "WHERE dataset = %s", DATASET))
    assert set(entities) == {"transaction", "account", "customer", "bank", "branch"}
    # Personal data: no min or max in the metadata.
    pii = query(database, "SELECT min_value, max_value FROM meta.column "
                          "WHERE dataset = %s AND is_pii", DATASET)
    assert pii and all(lo is None and hi is None for lo, hi in pii)

    [(facts,)] = query(database, "SELECT count(*) FROM gold.fact_transaction")
    assert facts == records
    for entity in ("account", "customer", "bank", "branch"):
        [(rows,)] = query(database, f"SELECT count(*) FROM gold.dim_{entity}")
        assert rows == entities[entity]
    [(days, first, last)] = query(
        database, "SELECT count(*), min(date_key), max(date_key) FROM gold.dim_date")
    [(lo, hi)] = query(database, "SELECT min(transaction_date), max(transaction_date) "
                                 "FROM gold.fact_transaction")
    assert first <= lo and last >= hi and days == (last - first).days + 1


def test_semantic_layer_metrics_are_stored(run: Run, database: str) -> None:
    """dbt + MetricFlow compute the metrics from gold and the DQ tables;
    they match gold and the DQ tables."""

    stored = dq("semantic")
    assert "headline" in stored.stdout

    headlines = dict(query(database, """
        SELECT area = 'Data Quality', count(*) FILTER (WHERE headline)
        FROM semantic.metric_definition GROUP BY 1"""))
    assert headlines == {False: 15, True: 10}
    definitions = {metric for (metric,) in query(database,
                                                 "SELECT metric FROM semantic.metric_definition")}
    assert {"transactions", "digital_share", "potential_cost_savings_egp",
            "failure_rate", "transaction_value_egp_yoy_growth",
            "initial_dq_score", "final_dq_score", "auto_fix_rate",
            "rule_failure_rate"} <= definitions

    def total(metric: str) -> float:
        [(value,)] = query(database, "SELECT value FROM semantic.metric_value "
                                     "WHERE metric = %s AND grain = 'total'", metric)
        return float(value)

    [(facts, value, failed, digital, customers)] = query(database, """
        SELECT count(*), sum(amount_egp) FILTER (WHERE transaction_status = 'Completed'),
               avg((transaction_status = 'Failed')::int),
               avg(coalesce(digital_channel_flag, false)::int), count(DISTINCT customer_id)
        FROM gold.fact_transaction""")
    assert total("transactions") == facts
    assert total("transaction_value_egp") == pytest.approx(float(value), abs=0.01)
    assert total("failure_rate") == pytest.approx(float(failed), abs=1e-6)
    assert total("digital_share") == pytest.approx(float(digital), abs=1e-6)
    assert total("active_customers") == customers

    # Monthly values add up to the total; every month is a calendar month.
    [(months, monthly)] = query(database, """
        SELECT count(*), sum(value) FROM semantic.metric_value
        WHERE metric = 'transactions' AND grain = 'month'""")
    assert months > 0 and float(monthly) == facts
    [(reported,)] = query(database, "SELECT count(*) FROM semantic.metric_report")
    assert reported > 0

    # The rest of the Power BI measures: in the semantic layer too.
    [(accounts, completed, card, purchases)] = query(database, """
        SELECT count(DISTINCT account_id), avg((transaction_status = 'Completed')::int),
               avg((card_type IS NOT NULL)::int),
               sum(amount_egp) FILTER (WHERE transaction_status = 'Completed'
                                         AND transaction_type = 'Purchase')
        FROM gold.fact_transaction""")
    assert total("active_accounts") == accounts
    assert total("success_rate") == pytest.approx(float(completed), abs=1e-6)
    assert total("card_share") == pytest.approx(float(card), abs=1e-6)
    assert total("purchase_value_egp") == pytest.approx(float(purchases or 0), abs=0.01)
    assert total("net_flow_egp") == pytest.approx(
        total("inflow_value_egp") - total("outflow_value_egp"), abs=0.01)
    # Time-dependent measures are stored per month only.
    [(ytd_months, ytd_totals)] = query(database, """
        SELECT count(*) FILTER (WHERE grain = 'month'), count(*) FILTER (WHERE grain = 'total')
        FROM semantic.metric_value WHERE metric = 'transaction_value_ytd_egp'""")
    assert ytd_months == months and ytd_totals == 0

    # Data quality: the same figures as the DQ views, in total and per run.
    [(processed, valid, clean, failed, checked)] = query(database, """
        SELECT sum(processed), sum(valid), sum(valid + resolved),
               (SELECT sum(failed) FROM semantic.dq_rule_result WHERE round = 1),
               (SELECT sum(checked) FROM semantic.dq_rule_result WHERE round = 1)
        FROM semantic.dq_run WHERE is_latest_round""")
    assert total("processed_records") == float(processed)
    assert total("initial_dq_score") == pytest.approx(float(valid) / float(processed), abs=1e-6)
    assert total("final_dq_score") == pytest.approx(float(clean) / float(processed), abs=1e-6)
    assert total("rule_failure_rate") == pytest.approx(float(failed) / float(checked), abs=1e-6)

    per_run = dict(query(database, """
        SELECT metric, value FROM semantic.metric_value
        WHERE grain = 'run' AND run_id = %s""", run.run_id))
    [latest] = metrics_table(database, "dq_run", run.run_id).query("is_latest_round").to_dict(
        "records")
    assert float(per_run["initial_dq_score"]) == pytest.approx(
        float(latest["initial_dq_score"]), abs=1e-6)
    assert float(per_run["final_dq_score"]) == pytest.approx(
        float(latest["final_dq_score"]), abs=1e-6)
    assert float(per_run["unresolved_records"]) == latest["unresolved_records"]
    assert float(per_run["dq_score_uplift"]) > 0


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
            "revalidate", "merge", "metrics", "metadata", "gold", "semantic",
            "powerbi-refresh"} <= steps
    for step in steps:
        extra = ["--file", "x.csv"] if step == "import-corrections" else []
        assert parse_args([step, *extra]).step == step


def test_powerbi_refresh_is_skipped_when_not_configured() -> None:
    """Without the POWERBI_* settings the step exits with 99, which Airflow
    reports as skipped, so the DAGs run without Power BI."""

    env = {k: v for k, v in os.environ.items() if not k.startswith("POWERBI_")}
    result = subprocess.run(
        [sys.executable, str(ROOT / "services" / "etl-service" / "src" / "powerbi.py")],
        cwd=ROOT, env=env, capture_output=True, text=True,
    )
    assert result.returncode == 99, result.stdout + result.stderr
    assert "not configured" in result.stdout


def test_airflow_emails_success_and_failure(monkeypatch) -> None:
    """dq_notify sends one e-mail per outcome to DQ_ALERT_EMAIL, and none
    without the SMTP settings."""

    from datetime import datetime, timezone
    from types import SimpleNamespace

    sys.path.insert(0, str(ROOT / "airflow" / "dags"))
    import dq_notify

    sent = []

    class FakeSMTP:
        def __init__(self, host, port, timeout):
            assert (host, port) == ("smtp.example.com", 587)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def starttls(self, context):
            pass

        def login(self, user, password):
            assert (user, password) == ("sender@example.com", "app-password")

        def send_message(self, message):
            sent.append(message)

    monkeypatch.setattr(dq_notify.smtplib, "SMTP", FakeSMTP)
    for key in ("DQ_ALERT_EMAIL", "SMTP_USER", "SMTP_PASSWORD"):
        monkeypatch.delenv(key, raising=False)

    start = datetime(2026, 10, 13, 18, 0, tzinfo=timezone.utc)
    run = SimpleNamespace(dag_id="dq_publish", run_id="scheduled__2026-10-13T18:00:00+00:00",
                          start_date=start, end_date=start.replace(minute=26), conf={})
    dq_notify.dag_succeeded({"dag_run": run})
    assert not sent   # not configured: nothing sent

    monkeypatch.setenv("DQ_ALERT_EMAIL", "owner@example.com")
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_USER", "sender@example.com")
    monkeypatch.setenv("SMTP_PASSWORD", "app-password")

    dq_notify.dag_succeeded({"dag_run": run})
    ti = SimpleNamespace(dag_id="dq_publish", task_id="merge", run_id=run.run_id, try_number=2)
    dq_notify.task_failed({"ti": ti, "dag_run": run,
                           "exception": RuntimeError("exit code 2")})

    success, failure = sent
    assert success["To"] == "owner@example.com"
    assert success["Subject"] == "[Transight] SUCCESS: dq_publish"
    assert "0h 26m 0s" in success.get_content()
    assert failure["Subject"] == "[Transight] FAILED: dq_publish (task merge)"
    assert "exit code 2" in failure.get_content()
    assert "/dags/dq_publish/runs/" in failure.get_content()
