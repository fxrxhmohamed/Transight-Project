"""DQ metric views for Power BI: apply sql/04_metrics.sql and check them.

The views (metrics.dq_run, dq_rule_result, dq_quarantine, dq_correction)
read the dq schema directly, so they always cover every run; this step
creates them again from sql/04_metrics.sql (so a change there reaches the
database) and checks what they show:

    errors (exit code 1)
        dq_run           one row per run and round; exactly one latest
                         round per run; resolved + discarded + unresolved
                         = quarantined in every round
        dq_rule_result   one row per run, round and rule
        dq_quarantine    status is OPEN, RESOLVED, MANUAL_REVIEW_REQUIRED
                         or DISCARDED
        dq_correction    correction_type is automatic or manual
    warnings
        dq_correction    an outcome dq_run does not know (auto-fixed counts
                         remediated and nullified_in_accepted only)

Usage
-----
    python src/metrics.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Shared code: the database connection and the schema files.
sys.path.append(str(Path(__file__).resolve().parents[2] / "common" / "src"))
import bronze  # noqa: E402
import schemas  # noqa: E402

SQL_FILE = Path(__file__).resolve().parents[3] / "sql" / "04_metrics.sql"

VIEWS = ("dq_run", "dq_rule_result", "dq_quarantine", "dq_correction")

OUTCOMES = (
    "remediated", "not_fixed", "nullified_in_accepted", "superseded", "resolved",
    "failed_revalidation", "manual_review_required", "discarded", "pending",
)

# Name -> query returning the failing rows.
ERRORS = {
    "dq_run: one row per run and round": """
        SELECT dataset, run_id, round FROM metrics.dq_run
        GROUP BY dataset, run_id, round HAVING count(*) > 1
    """,
    "dq_run: one latest round per run": """
        SELECT dataset, run_id FROM metrics.dq_run
        GROUP BY dataset, run_id
        HAVING count(*) FILTER (WHERE is_latest_round) <> 1
    """,
    "dq_run: quarantined = resolved + discarded + unresolved": """
        SELECT dataset, run_id, round FROM metrics.dq_run
        WHERE resolved + discarded + unresolved_records <> quarantined
    """,
    "dq_run: run_id, dataset and round are set": """
        SELECT dataset, run_id, round FROM metrics.dq_run
        WHERE run_id IS NULL OR dataset IS NULL OR round IS NULL
    """,
    "dq_rule_result: one row per run, round and rule": """
        SELECT dataset, run_id, round, rule_id FROM metrics.dq_rule_result
        GROUP BY dataset, run_id, round, rule_id HAVING count(*) > 1
    """,
    "dq_rule_result: rule_id is set": """
        SELECT dataset, run_id, round FROM metrics.dq_rule_result WHERE rule_id IS NULL
    """,
    "dq_quarantine: known status": """
        SELECT DISTINCT status FROM metrics.dq_quarantine
        WHERE status IS NULL
           OR status NOT IN ('OPEN', 'RESOLVED', 'MANUAL_REVIEW_REQUIRED', 'DISCARDED')
    """,
    "dq_correction: correction_type is automatic or manual": """
        SELECT DISTINCT correction_type FROM metrics.dq_correction
        WHERE correction_type NOT IN ('automatic', 'manual')
    """,
}

WARNINGS = {
    "dq_correction: known outcome (dq_run's auto-fixed count may need a look)": f"""
        SELECT DISTINCT outcome FROM metrics.dq_correction
        WHERE outcome IS NULL OR outcome NOT IN ({", ".join(f"'{o}'" for o in OUTCOMES)})
    """,
}


def main() -> int:
    with bronze.connect() as conn:
        schemas.ensure(conn, "02_dq.sql")
        with conn.transaction():
            conn.execute(SQL_FILE.read_text(encoding="utf-8"))

        for view in VIEWS:
            rows = conn.execute(f"SELECT count(*) FROM metrics.{view}").fetchone()[0]
            print(f"  metrics.{view:<16} {rows:>9} rows")

        failed = 0
        for checks, label in ((ERRORS, "ERROR"), (WARNINGS, "WARN")):
            for name, sql in checks.items():
                bad = conn.execute(sql).fetchall()
                if bad:
                    print(f"{label}: {name}: {len(bad)} failing, e.g. {bad[:3]}",
                          file=sys.stderr)
                    failed += label == "ERROR"

    if failed:
        print(f"{failed} metric check(s) failed.", file=sys.stderr)
        return 1
    print("Metric views up to date; all checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
