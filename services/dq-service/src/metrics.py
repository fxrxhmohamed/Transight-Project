"""Lineage and DQ metrics: four tables in PostgreSQL for Power BI (PySpark).

Built from what every step stores in the dq schema (the summary of every
round, remediation audits, the quarantine history, corrections and the
correction log), for every run of the dataset:

    dq_run          one row per run and round: record counts and DQ score
    dq_rule_result  one row per run, round and rule: checked, failed,
                    repaired, unresolved
    dq_quarantine   status history of every quarantined record
    dq_correction   every automatic repair (remediation audit) and manual
                    correction (correction log), with its outcome

stored in metrics.<table>. The rows of the runs being measured are
replaced each time, so the tables always match the dq schema. The large
inputs (repair audits, the ledger history, the correction log) are read
and joined in Spark; the per-round counts come from the round results.

Definitions (dq_run)
--------------------
    processed               rows validated (input minus exact duplicates)
    valid                   accepted on the first validation
    auto_fixed_records      valid records that needed at least one
                            automatic repair
    quarantined             failed the first validation
    records_with_issues     auto_fixed_records + quarantined
    manually_fixed_records  quarantined, corrected by hand and RESOLVED
    confirmed_records       held because of many automatic repairs,
                            accepted as they were by a person (ACCEPT)
                            and RESOLVED
    auto_resolved_records   RESOLVED without a manual fix or confirmation
                            (re-validated with --all, e.g. after a rule
                            change)
    unresolved_records      still OPEN or MANUAL_REVIEW_REQUIRED
    initial_dq_score        valid / processed
    final_dq_score          (valid + resolved) / processed

Counts after round 1 are as of the end of that round, so the latest round
of a run reads e.g. 350 issues -> 200 auto-fixed -> 130 manually fixed ->
20 unresolved (plus confirmed, auto-resolved and discarded records).

Usage
-----
    python src/metrics.py                 # every run
    python src/metrics.py --run ING_20261011_001
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from psycopg import sql
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import db
from dq_engine import DEFAULT_RULES
from layout import (
    AUDIT_COLUMNS,
    DISCARDED,
    MANUAL_REVIEW,
    OPEN,
    RESOLVED,
    Layout,
    LayoutError,
    list_runs,
)
from registry import ConfigError, load_registry
from values import lit

spark_io = db.spark_io

TABLES = ("dq_run", "dq_rule_result", "dq_quarantine", "dq_correction")

# Audit outcomes that count as an automatic fix.
AUTO_FIX_OUTCOMES = ("remediated", "nullified_in_accepted")

RUN_SCHEMA = (
    "run_id string, dataset string, source_file string, ingested_at string, "
    "schema_version string, rules_version string, input_rows long, "
    "duplicate_rows_removed long, processed long, valid long, valid_with_warnings long, "
    "auto_fixed_records long, quarantined long, records_with_issues long, "
    "initial_dq_score double, round int, round_type string, revalidated_round int, "
    "started_at string, finished_at string, round_open_before long, "
    "round_revalidated long, round_resolved long, round_failed_again long, "
    "round_manual_review long, round_discarded long, resolved long, "
    "manually_fixed_records long, confirmed_records long, auto_resolved_records long, "
    "discarded long, manual_review long, open long, unresolved_records long, "
    "final_dq_score double"
)
RULE_SCHEMA = (
    "run_id string, dataset string, round int, round_type string, rule_id string, "
    "rule_name string, rule_type string, column string, dimension string, "
    "severity string, error_code string, error_category string, origin string, "
    "checked long, failed long, repaired long, unresolved long"
)

MANUAL_OUTCOMES = {
    RESOLVED: "resolved",
    OPEN: "failed_revalidation",
    MANUAL_REVIEW: "manual_review_required",
    DISCARDED: "discarded",
}


def _frame(rows: list[dict[str, Any]], schema: str) -> DataFrame:
    """Rows (dicts) as a Spark DataFrame with the given schema."""

    names = [field.strip().split(" ")[0] for field in schema.split(",")]
    return spark_io.local_frame([tuple(row.get(n) for n in names) for row in rows], schema)


# ============================================================
# Reading a run
# ============================================================

def read_rounds(layout: Layout) -> dict[int, dict[str, Any]]:
    """Summary of every round, by round number."""

    rounds = layout.load_results()
    if 1 not in rounds:
        raise LayoutError(f"Run {layout.run_id} has no round 1 result")
    return rounds


def score(part: int, whole: int) -> float:
    return round(part / whole, 6) if whole else 0.0


def _record_ids(layout: Layout, kind: str, round_number: int) -> DataFrame:
    table = db.qualified(*db.record_table_name(layout.dataset, kind))
    return spark_io.read(
        f"SELECT record_id FROM {table} WHERE run_id = {db.literal(layout.run_id)} "
        f"AND round = {int(round_number)}",
    )


# ============================================================
# Tables
# ============================================================

def run_table(layout: Layout, rounds: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    first = rounds[1]
    counts = first["counts"]
    processed = counts["processed_rows"]
    valid = counts["accepted_rows"]
    quarantined = counts["quarantined_rows"]

    outcomes = ", ".join(lit(o) for o in AUTO_FIX_OUTCOMES)
    fixed = layout.spark_table("dq.remediation_audit", ["record_id"], 1,
                               where=f"outcome IN ({outcomes})")
    auto_fixed = (
        fixed.distinct()
        .join(_record_ids(layout, "quarantine", 1), "record_id", "left_anti")
        .count()
    )

    base = {
        "run_id": layout.run_id,
        "dataset": layout.dataset,
        "source_file": first.get("source_file"),
        "ingested_at": (first.get("ingestion") or {}).get("ingested_at"),
        "schema_version": str(first.get("config", {}).get("schema_version")),
        "rules_version": str(first.get("config", {}).get("rules_version")),
        "input_rows": counts["input_rows"],
        "duplicate_rows_removed": counts["duplicate_rows_removed"],
        "processed": processed,
        "valid": valid,
        "valid_with_warnings": counts["accepted_with_warnings_rows"],
        "auto_fixed_records": auto_fixed,
        "quarantined": quarantined,
        "records_with_issues": auto_fixed + quarantined,
        "initial_dq_score": score(valid, processed),
    }

    rows = []
    resolved_total = manual_fixed = confirmed = 0
    discarded_total = manual_review_total = 0
    open_records = quarantined

    for number, result in rounds.items():
        row = {**base, "round": number,
               "started_at": result.get("started_at"),
               "finished_at": result.get("finished_at")}

        if number == 1:
            row.update(round_type="validation", revalidated_round=None)
            round_counts: dict[str, Any] = {}
        else:
            round_counts = result["counts"]
            k = result["revalidated_round"]
            corrections = layout.load_corrections(k)

            def ids_with(action: str) -> set[str]:
                return set(corrections.loc[corrections["action"].eq(action), "record_id"])

            fixed_ids = ids_with("FIX")
            accepted_ids = ids_with("ACCEPT") - fixed_ids
            resolved_ids = (
                {r["record_id"] for r in _record_ids(layout, "resolved", k).collect()}
                if layout.has_records("resolved") else set()
            )

            resolved_total += round_counts["resolved"]
            manual_fixed += len(resolved_ids & fixed_ids)
            confirmed += len(resolved_ids & accepted_ids)
            discarded_total += (
                round_counts["discarded"] + round_counts["removed_as_duplicate"]
            )
            manual_review_total += round_counts["manual_review_required"]
            open_records = round_counts["open_after"]
            row.update(round_type="revalidation", revalidated_round=k)

        row.update(
            round_open_before=round_counts.get("open_before"),
            round_revalidated=round_counts.get("revalidated"),
            round_resolved=round_counts.get("resolved"),
            round_failed_again=round_counts.get("failed_again"),
            round_manual_review=round_counts.get("manual_review_required"),
            round_discarded=(
                round_counts["discarded"] + round_counts["removed_as_duplicate"]
                if round_counts else None
            ),
            resolved=resolved_total,
            manually_fixed_records=manual_fixed,
            confirmed_records=confirmed,
            auto_resolved_records=resolved_total - manual_fixed - confirmed,
            discarded=discarded_total,
            manual_review=manual_review_total,
            open=open_records,
            unresolved_records=open_records + manual_review_total,
            final_dq_score=score(valid + resolved_total, processed),
        )
        if row["resolved"] + row["discarded"] + row["unresolved_records"] != quarantined:
            raise ConfigError(
                f"Run {layout.run_id}: resolved + discarded + unresolved does not "
                f"add up to the {quarantined} quarantined records."
            )
        rows.append(row)
    return rows


def rule_table(layout: Layout, rounds: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for number, result in rounds.items():
        for stats in result.get("rules", []):
            rows.append({
                "run_id": layout.run_id,
                "dataset": layout.dataset,
                "round": number,
                "round_type": "validation" if number == 1 else "revalidation",
                "rule_id": stats["rule_id"],
                "rule_name": stats.get("name"),
                "rule_type": stats.get("type"),
                "column": stats.get("column"),
                "dimension": stats.get("dimension"),
                "severity": stats.get("severity"),
                "error_code": stats.get("error_code"),
                "error_category": stats.get("error_category"),
                "origin": stats.get("origin"),
                "checked": stats["checked"],
                "failed": stats["detected"],
                "repaired": stats["remediated"],
                "unresolved": stats["unresolved"],
            })
    return rows


def _history(layout: Layout) -> DataFrame:
    return layout.spark_table(
        "dq.quarantine_history",
        ["history_id", "record_id", "status", "round", "attempts", "changed_at"],
    )


def quarantine_table(layout: Layout) -> DataFrame:
    """Every status change of every quarantined record, numbered per record."""

    return _history(layout).selectExpr(
        f"{lit(layout.run_id)} AS run_id", f"{lit(layout.dataset)} AS dataset",
        "record_id",
        "CAST(row_number() OVER (PARTITION BY record_id ORDER BY history_id) AS INT) "
        "AS sequence",
        "round", "status",
        "lag(status) OVER (PARTITION BY record_id ORDER BY history_id) AS previous_status",
        "attempts", "changed_at",
    )


def correction_table(layout: Layout, rounds: dict[int, dict[str, Any]]) -> DataFrame:
    """Automatic repairs and manual corrections, with their outcome."""

    finished = spark_io.local_frame(
        [(n, r.get("finished_at"), r.get("revalidated_round")) for n, r in rounds.items()],
        "round int, finished_at string, revalidated_round int",
    ).selectExpr("round", "CAST(finished_at AS TIMESTAMP) AS finished_at",
                 "revalidated_round")

    # ---- Automatic repairs: the audit of every round ----
    automatic = (
        layout.spark_table("dq.remediation_audit", ["round", *AUDIT_COLUMNS])
        .join(F.broadcast(finished.select("round", "finished_at")), "round", "left")
        .selectExpr(
            "round", "record_id", "`column`", "'automatic' AS correction_type", "action",
            "rule_id", "original_value AS old_value", "cleaned_value AS new_value",
            "outcome", "CAST(NULL AS STRING) AS reason", "'dq_engine' AS `user`",
            "finished_at AS corrected_at",
        )
    )

    # ---- Manual corrections: the record's status right after its round
    # was re-validated ----
    after = (
        _history(layout)
        .join(F.broadcast(finished.filter("revalidated_round IS NOT NULL")),
              F.col("changed_at") == F.col("finished_at"))
        .selectExpr("record_id", "revalidated_round AS round", "status AS status_after")
        .dropDuplicates(["record_id", "round"])
    )
    outcome = "CASE " + " ".join(
        f"WHEN status_after = {lit(status)} THEN {lit(name)}"
        for status, name in MANUAL_OUTCOMES.items()
    ) + " ELSE 'pending' END"
    log = spark_io.read(
        "SELECT log_id, round, record_id, \"column\", old_value, new_value, action, "
        "reason, \"user\", corrected_at FROM dq.correction_log "
        f"WHERE dataset = {db.literal(layout.dataset)} AND run_id = {db.literal(layout.run_id)}"
    )
    manual = log.join(after, ["record_id", "round"], "left").selectExpr(
        "round", "record_id", "`column`", "'manual' AS correction_type", "action",
        "CAST(NULL AS STRING) AS rule_id", "old_value", "new_value",
        # A field corrected again in the same round: only the newest line
        # was applied.
        "CASE WHEN row_number() OVER (PARTITION BY round, record_id, `column`, action "
        f"ORDER BY log_id DESC) > 1 THEN 'superseded' ELSE {outcome} END AS outcome",
        "reason", "`user`", "corrected_at",
    )

    return automatic.unionByName(manual).selectExpr(
        f"{lit(layout.run_id)} AS run_id", f"{lit(layout.dataset)} AS dataset", "*",
    )


def build_tables(layout: Layout) -> dict[str, Any]:
    rounds = read_rounds(layout)
    return {
        "dq_run": run_table(layout, rounds),
        "dq_rule_result": rule_table(layout, rounds),
        "dq_quarantine": quarantine_table(layout),
        "dq_correction": correction_table(layout, rounds),
    }


# ============================================================
# Main
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild the DQ metric tables (metrics.dq_run, "
        "dq_rule_result, dq_quarantine, dq_correction) for Power BI.",
    )
    parser.add_argument("--run", default="all",
                        help="Run (ingestion) id, or 'all' (default).")
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--schema", type=Path, default=None)
    return parser.parse_args()


def export_metrics(dataset: str, runs: list[str] | None = None) -> dict[str, Any]:
    """Build the four tables for some runs (default: all) and store them in
    metrics.<table>, replacing those runs' rows. Returns the dq_run rows and
    the number of rows per table."""

    every_run = runs is None
    runs = runs or list_runs(dataset)
    if not runs:
        raise LayoutError(f"No validated runs for {dataset}")

    built = [build_tables(Layout(dataset, run_id)) for run_id in runs]
    run_rows = [row for tables in built for row in tables["dq_run"]]
    frames = {
        "dq_run": _frame(run_rows, RUN_SCHEMA),
        "dq_rule_result": _frame(
            [row for tables in built for row in tables["dq_rule_result"]], RULE_SCHEMA),
    }
    for name in ("dq_quarantine", "dq_correction"):
        frame = built[0][name]
        for tables in built[1:]:
            frame = frame.unionByName(tables[name])
        frames[name] = frame

    with db.transaction() as conn:
        for name in TABLES:
            table = sql.Identifier("metrics", name)
            if every_run:
                db.delete(conn, table, {"dataset": dataset})
            else:
                for run_id in runs:
                    db.delete(conn, table, {"dataset": dataset, "run_id": run_id})

    counts = {}
    for name, frame in frames.items():
        frame = frame.cache()
        counts[name] = frame.count()
        spark_io.write(frame, f"metrics.{name}")
        frame.unpersist()
    return {"dq_run": run_rows, "counts": counts}


def main() -> int:
    args = parse_args()
    registry = load_registry(args.rules, args.schema)
    dataset = registry.dataset.get("name", "dataset")

    written = export_metrics(dataset, None if args.run == "all" else [args.run])

    last_rows: dict[str, dict[str, Any]] = {}
    for row in written["dq_run"]:
        last_rows[row["run_id"]] = row
    for run_id, last in last_rows.items():
        print(
            f"{run_id} round {last['round']}: {last['records_with_issues']} "
            f"records with issues -> {last['auto_fixed_records']} auto-fixed "
            f"-> {last['manually_fixed_records']} manually fixed -> "
            f"{last['unresolved_records']} unresolved "
            f"({last['confirmed_records']} confirmed after review, "
            f"{last['auto_resolved_records']} resolved otherwise, "
            f"{last['discarded']} discarded). DQ score "
            f"{last['initial_dq_score']:.2%} -> {last['final_dq_score']:.2%}"
        )

    for name, rows in written["counts"].items():
        print(f"  metrics.{name:<15} {rows:>9} rows")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ConfigError, LayoutError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
    finally:
        spark_io.stop()
