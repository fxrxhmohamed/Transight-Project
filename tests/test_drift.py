import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "metadata-service" / "src"))
import drift  # noqa: E402

from test_pipeline import SAMPLE, connect, database, dq  # noqa: E402,F401

RULES = drift.load_rules(ROOT / "config" / "drift" / "drift_rules.yaml")


def profile(rows=1000, channels=("atm", "branch"), email_missing=0.0, mean=100.0):
    return {"row_count": rows, "columns": [
        {"column_name": "channel", "logical_type": "categorical",
         "missing_percentage": 0.0, "category_values": list(channels)},
        {"column_name": "email", "logical_type": "email", "missing_percentage": email_missing},
        {"column_name": "transaction_amount", "logical_type": "float",
         "missing_percentage": 0.0, "mean": mean}]}


def found(old, new):
    return {(f["check_name"], f["column_name"], f["severity"])
            for f in drift.compare(old, new, RULES)}


def test_identical_profiles_have_no_drift():
    assert found(profile(), profile()) == set()


def test_small_changes_are_ignored():
    assert found(profile(), profile(rows=900, mean=110.0, email_missing=2.0)) == set()


def test_half_the_rows_is_critical():
    assert ("row_count", None, "critical") in found(profile(), profile(rows=450))


def test_new_channel_is_critical():
    assert ("new_category", "channel", "critical") in found(
        profile(), profile(channels=("atm", "branch", "crypto wallet")))


def test_null_jump_is_critical():
    assert ("null_rate", "email", "critical") in found(profile(), profile(email_missing=30.0))


def test_mean_triple_is_critical():
    assert ("mean_change", "transaction_amount", "critical") in found(profile(), profile(mean=300.0))


def test_profile_without_categories_is_skipped():
    old = profile()
    del old["columns"][0]["category_values"]
    assert not any(c == "new_category" for c, _, _ in found(old, profile(channels=("x",))))


# ---- integration: ingest, profile and drift on the 50-row sample ----

def test_drift_flags_row_count_drop_end_to_end(database, tmp_path):
    sample = pd.read_csv(SAMPLE, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    ids = []
    for name, rows in (("big", 40), ("small", 15)):
        path = tmp_path / f"{name}.csv"
        sample.iloc[:rows].to_csv(path, index=False)
        run_id = dq("ingest", "--input", path).stdout.strip().splitlines()[-1]
        dq("profile", "--run", run_id)
        dq("drift", "--run", run_id, "--no-email")
        ids.append(run_id)

    with connect(database) as conn:
        status = dict(conn.execute("SELECT run_id, status FROM dq.drift_check").fetchall())
        rows = conn.execute("SELECT check_name, severity FROM dq.drift_finding WHERE run_id = %s",
                            (ids[1],)).fetchall()
    assert status[ids[0]] == "BASELINE"
    assert status[ids[1]] == "CRITICAL"
    assert ("row_count", "critical") in rows