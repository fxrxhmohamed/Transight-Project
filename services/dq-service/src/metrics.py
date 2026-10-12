"""Lineage and DQ metrics: four tables in PostgreSQL for Power BI.

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
replaced each time, so the tables always match the dq schema.

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

import pandas as pd
from psycopg import sql

import db
from dq_engine import DEFAULT_RULES
from layout import (
    DISCARDED,
    MANUAL_REVIEW,
    OPEN,
    RESOLVED,
    Layout,
    LayoutError,
    list_runs,
    load_history,
)
from registry import ConfigError, load_registry


TABLES = ("dq_run", "dq_rule_result", "dq_quarantine", "dq_correction")

# Audit outcomes that count as an automatic fix.
AUTO_FIX_OUTCOMES = ("remediated", "nullified_in_accepted")

RULE_COLUMNS = [
    "run_id", "dataset", "round", "round_type", "rule_id", "rule_name",
    "rule_type", "column", "dimension", "severity", "error_code",
    "error_category", "origin", "checked", "failed", "repaired",
    "unresolved",
]
QUARANTINE_COLUMNS = [
    "run_id", "dataset", "record_id", "sequence", "round", "status",
    "previous_status", "attempts", "changed_at",
]
CORRECTION_COLUMNS = [
    "run_id", "dataset", "round", "record_id", "column", "correction_type",
    "action", "rule_id", "old_value", "new_value", "outcome", "reason",
    "user", "corrected_at",
]

MANUAL_OUTCOMES = {
    RESOLVED: "resolved",
    OPEN: "failed_revalidation",
    MANUAL_REVIEW: "manual_review_required",
    DISCARDED: "discarded",
}


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


# ============================================================
# Tables
# ============================================================

def run_table(
    layout: Layout,
    rounds: dict[int, dict[str, Any]],
) -> pd.DataFrame:
    first = rounds[1]
    counts = first["counts"]
    processed = counts["processed_rows"]
    valid = counts["accepted_rows"]
    quarantined = counts["quarantined_rows"]

    quarantined_ids = set(layout.load_quarantine(1, columns=[])["record_id"])

    audit = layout.load_audit(1)
    fixed = audit[audit["outcome"].isin(AUTO_FIX_OUTCOMES)]
    auto_fixed = int(
        fixed.loc[~fixed["record_id"].isin(quarantined_ids), "record_id"].nunique()
    )

    base = {
        "run_id": layout.run_id,
        "dataset": layout.dataset,
        "source_file": first.get("source_file"),
        "ingested_at": (first.get("ingestion") or {}).get("ingested_at"),
        "schema_version": first.get("config", {}).get("schema_version"),
        "rules_version": first.get("config", {}).get("rules_version"),
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

    resolved_by_round = dict(layout.load_resolved())
    rows = []
    resolved_total = manual_fixed = confirmed = 0
    discarded_total = manual_review_total = 0
    open_records = quarantined

    for number, result in rounds.items():
        row = {**base, "round": number}

        if number == 1:
            row.update(
                round_type="validation",
                revalidated_round=None,
                started_at=result.get("started_at"),
                finished_at=result.get("finished_at"),
            )
            round_counts = {}
        else:
            round_counts = result["counts"]
            k = result["revalidated_round"]
            corrections = layout.load_corrections(k)

            def ids_with(action: str) -> set[str]:
                return set(corrections.loc[corrections["action"].eq(action), "record_id"])

            fixed_ids = ids_with("FIX")
            accepted_ids = ids_with("ACCEPT") - fixed_ids
            resolved_ids = (
                set(resolved_by_round[k]["record_id"]) if k in resolved_by_round
                else set()
            )

            resolved_total += round_counts["resolved"]
            manual_fixed += len(resolved_ids & fixed_ids)
            confirmed += len(resolved_ids & accepted_ids)
            discarded_total += (
                round_counts["discarded"] + round_counts["removed_as_duplicate"]
            )
            manual_review_total += round_counts["manual_review_required"]
            open_records = round_counts["open_after"]

            row.update(
                round_type="revalidation",
                revalidated_round=k,
                started_at=result.get("started_at"),
                finished_at=result.get("finished_at"),
            )

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
        rows.append(row)

    table = pd.DataFrame(rows)
    integer_columns = [
        "revalidated_round", "round_open_before", "round_revalidated",
        "round_resolved", "round_failed_again", "round_manual_review",
        "round_discarded",
    ]
    table[integer_columns] = table[integer_columns].astype("Int64")

    balance = table["resolved"] + table["discarded"] + table["unresolved_records"]
    if not balance.eq(quarantined).all():
        raise ConfigError(
            f"Run {layout.run_id}: resolved + discarded + unresolved does not "
            f"add up to the {quarantined} quarantined records."
        )
    return table


def rule_table(layout: Layout, rounds: dict[int, dict[str, Any]]) -> pd.DataFrame:
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
    return pd.DataFrame(rows, columns=RULE_COLUMNS)


def quarantine_table(layout: Layout, history: pd.DataFrame) -> pd.DataFrame:
    history = history.reset_index(drop=True)
    by_record = history.groupby("record_id", sort=False)
    history = history.assign(
        run_id=layout.run_id,
        dataset=layout.dataset,
        sequence=by_record.cumcount() + 1,
        previous_status=by_record["status"].shift(),
    )
    return history.sort_values(
        ["record_id", "sequence"], ignore_index=True,
    )[QUARANTINE_COLUMNS]


def correction_table(
    layout: Layout,
    rounds: dict[int, dict[str, Any]],
    history: pd.DataFrame,
) -> pd.DataFrame:
    parts = []

    # ---- Automatic repairs ----
    for number, result in rounds.items():
        audit = layout.load_audit(number)
        if not len(audit):
            continue
        parts.append(pd.DataFrame({
            "round": number,
            "record_id": audit["record_id"],
            "column": audit["column"],
            "correction_type": "automatic",
            "action": audit["action"],
            "rule_id": audit["rule_id"],
            "old_value": audit["original_value"],
            "new_value": audit["cleaned_value"],
            "outcome": audit["outcome"],
            "reason": pd.NA,
            "user": "dq_engine",
            "corrected_at": result.get("finished_at"),
        }))

    # ---- Manual corrections ----
    log = layout.load_log()
    if len(log):
        log = log.assign(round=log["round"].astype(int))

        # The record's status right after its round was re-validated.
        after = []
        for result in rounds.values():
            if "revalidated_round" not in result:
                continue
            changed = history[history["changed_at"].eq(result["finished_at"])]
            after.append(pd.DataFrame({
                "record_id": changed["record_id"],
                "round": result["revalidated_round"],
                "status_after": changed["status"],
            }))
        outcome = (
            pd.concat(after, ignore_index=True).drop_duplicates(["record_id", "round"])
            if after else pd.DataFrame(columns=["record_id", "round", "status_after"])
        )
        log = log.merge(
            outcome.astype({"round": int}), on=["record_id", "round"], how="left",
        )
        log["outcome"] = log["status_after"].map(MANUAL_OUTCOMES).fillna("pending")

        # A field corrected again in the same round: only the newest line
        # was applied.
        superseded = log.duplicated(
            ["round", "record_id", "column", "action"], keep="last",
        )
        log.loc[superseded, "outcome"] = "superseded"

        parts.append(log.assign(
            correction_type="manual", rule_id=pd.NA,
        ).drop(columns="status_after"))

    if not parts:
        return pd.DataFrame(columns=CORRECTION_COLUMNS)

    table = pd.concat(parts, ignore_index=True)
    table["run_id"] = layout.run_id
    table["dataset"] = layout.dataset
    return table[CORRECTION_COLUMNS]


def build_tables(layout: Layout) -> dict[str, pd.DataFrame]:
    rounds = read_rounds(layout)
    history = load_history(layout)
    return {
        "dq_run": run_table(layout, rounds),
        "dq_rule_result": rule_table(layout, rounds),
        "dq_quarantine": quarantine_table(layout, history),
        "dq_correction": correction_table(layout, rounds, history),
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


def export_metrics(
    dataset: str,
    runs: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Build the four tables for some runs (default: all) and store them in
    metrics.<table>, replacing those runs' rows."""

    every_run = runs is None
    runs = runs or list_runs(dataset)
    if not runs:
        raise LayoutError(f"No validated runs for {dataset}")

    parts: dict[str, list[pd.DataFrame]] = {name: [] for name in TABLES}
    for run_id in runs:
        for name, frame in build_tables(Layout(dataset, run_id)).items():
            if len(frame):
                parts[name].append(frame)

    written = {}
    with db.transaction() as conn:
        for name, frames in parts.items():
            table = sql.Identifier("metrics", name)
            if every_run:
                db.delete(conn, table, {"dataset": dataset})
            else:
                for run_id in runs:
                    db.delete(conn, table, {"dataset": dataset, "run_id": run_id})
            frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            db.write_frame(conn, table, frame)
            written[name] = frame
    return written


def main() -> int:
    args = parse_args()
    registry = load_registry(args.rules, args.schema)
    dataset = registry.dataset.get("name", "dataset")

    written = export_metrics(dataset, None if args.run == "all" else [args.run])

    runs = written["dq_run"]
    for run_id, rows in runs.groupby("run_id", sort=False):
        last = rows.iloc[-1]
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

    for name, table in written.items():
        print(f"  metrics.{name:<15} {len(table):>9} rows")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ConfigError, LayoutError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
