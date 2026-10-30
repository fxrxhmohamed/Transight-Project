"""DQ tables of the semantic layer: build them with dbt and check them.

    dbt build --select tag:dq   (dbt/models/dq, dbt/tests)

loads the four DQ tables into the semantic schema, from what the DQ engine
recorded in the dq schema, and runs their tests:

    semantic.dq_run          one row per run and round: record counts and
                             DQ scores
    semantic.dq_rule_result  one row per run, round and rule: checked,
                             failed, repaired, unresolved
    semantic.dq_quarantine   every status change of every quarantined record
    semantic.dq_correction   every automatic repair and manual correction,
                             with its outcome

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

The DQ metrics (scores, rates) are defined over these tables in the same
dbt project (python dq.py semantic). The views of earlier versions
(metrics.dq_*, dq.validated_round, from sql/04_metrics.sql) are dropped.

Usage
-----
    python src/metrics.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Shared code: the database connection, the schema files, dbt.
sys.path.append(str(Path(__file__).resolve().parents[2] / "common" / "src"))
import bronze  # noqa: E402
import dbt_project  # noqa: E402
import schemas  # noqa: E402

TABLES = ("dq_run", "dq_rule_result", "dq_quarantine", "dq_correction")


def drop_old_views(conn) -> None:
    """The metrics schema of earlier versions (views or tables) and its
    shared view: the DQ tables now live in the semantic schema."""

    with conn.transaction():
        conn.execute("DROP VIEW IF EXISTS dq.validated_round CASCADE")
        for name in TABLES:
            kind = conn.execute(
                "SELECT relkind FROM pg_class WHERE oid = to_regclass(%s)",
                (f"metrics.{name}",),
            ).fetchone()
            if kind:
                conn.execute(f"DROP {'TABLE' if kind[0] == 'r' else 'VIEW'} "
                             f"metrics.{name} CASCADE")
        empty = conn.execute(
            "SELECT to_regnamespace('metrics') IS NOT NULL AND NOT EXISTS ("
            "SELECT 1 FROM pg_class WHERE relnamespace = to_regnamespace('metrics'))"
        ).fetchone()[0]
        if empty:
            conn.execute("DROP SCHEMA metrics")


def main() -> int:
    with bronze.connect() as conn:
        schemas.ensure(conn, "02_dq.sql")
        drop_old_views(conn)

    dbt, _ = dbt_project.executables()
    try:
        with dbt_project.workspace() as project:
            output = dbt_project.call(project, dbt, "build", "--profiles-dir", str(project),
                                      "--select", "tag:dq")
    except dbt_project.DbtError as error:
        print(f"Error: DQ tables or checks failed.\n{error}", file=sys.stderr)
        return 1

    for line in output.splitlines():
        if re.search(r"\bWARN\b", line) and "WARN=" not in line:
            print("WARN: " + re.sub(r"^\S+\s+", "", line).strip(), file=sys.stderr)

    with bronze.connect() as conn:
        for name in TABLES:
            rows = conn.execute(f"SELECT count(*) FROM semantic.{name}").fetchone()[0]
            print(f"  semantic.{name:<16} {rows:>9} rows")
    summary = re.findall(r"Done\. PASS=\d+.*", output)
    print(f"DQ tables loaded; checks passed ({summary[-1] if summary else 'dbt build'}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
