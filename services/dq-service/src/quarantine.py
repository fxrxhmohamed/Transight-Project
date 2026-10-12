"""Quarantine manager: manual correction and re-validation.

After dq_engine.py has validated a run, its quarantined records are
worked through in rounds:

    template    write correction_template.csv for the open records of
                the current round: one line per field to fix, with the
                current value and why it failed
    import      read the edited template; check every line and store the
                accepted corrections (dq.correction, dq.correction_log)
    revalidate  apply the corrections and run ALL rules again (Spark),
                together with the valid records (so uniqueness and one-
                value-per-customer are judged on the whole dataset)
                    pass             -> RESOLVED (dq.<dataset>_resolved)
                    fail             -> OPEN in the next round
                    fail, too often  -> MANUAL_REVIEW_REQUIRED
                    action DISCARD   -> DISCARDED
                Records without corrections move to the next round as
                they are.
    status      counts per status and round

Everything is stored in PostgreSQL (layout.py); the template is the only
file, written for people to fill in (e.g. in Excel).

The correction UI (correction_ui.py) calls the same import and
re-validation functions, so a fix gives the same result whichever way it
is entered.

Template columns you fill in
    new_value   the corrected value (__NULL__ to empty a field)
    action      blank or FIX to correct; DISCARD to drop the whole record
                (e.g. the second copy of a duplicated transaction);
                ACCEPT to confirm a record's automatic repairs as they are
                (records held by an auto_fix_review rule)
    reason      why (kept in the correction log)
Lines with neither new_value nor action are ignored. A column that the
template does not list can be corrected by adding a line for it.

Usage
-----
    python src/quarantine.py template   --run ING_20261011_001
    python src/quarantine.py import     --run ING_20261011_001 \\
        --file correction_template_ING_20261011_001_round_1.csv
    python src/quarantine.py revalidate --run ING_20261011_001
    python src/quarantine.py status     --run latest
"""

from __future__ import annotations

import argparse
import getpass
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Callable

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import db
from dq_engine import (
    ACCEPTED,
    DEFAULT_RULES,
    FOCUS,
    QUARANTINED,
    RID,
    ROW,
    validate_frame,
)
from layout import (
    AUDIT,
    CORRECTION_COLUMNS,
    DISCARDED,
    ERROR_COLUMNS,
    ERRORS,
    MANUAL_REVIEW,
    OPEN,
    RESOLVED,
    Layout,
    LayoutError,
    current_round,
    latest_run,
    load_status,
    now_utc,
    save_status,
    write_csv,
)
from registry import ConfigError, Registry, load_registry
from values import col

spark_io = db.spark_io

# __row of the corrected records: after every context row, so a corrected
# record identical to another row is the copy that is dropped.
TARGET_ROW_OFFSET = 1 << 62


NULL_MARKER = "__NULL__"
ACTIONS = {"FIX", "DISCARD", "ACCEPT"}
# Actions on the whole record (no column, no new value).
RECORD_ACTIONS = {"DISCARD", "ACCEPT"}

TEMPLATE_COLUMNS = [
    "record_id", "column", "current_value", "error_codes", "severity",
    "messages", "rule_ids", "new_value", "action", "reason",
]
DEFAULT_MAX_ATTEMPTS = 3


# ============================================================
# Helpers
# ============================================================

def open_layout(args: argparse.Namespace, registry: Registry) -> Layout:
    dataset = registry.dataset.get("name", "dataset")
    run_id = latest_run(dataset) if args.run == "latest" else args.run
    return Layout(dataset, run_id)


def require_round(status: pd.DataFrame) -> int:
    round_number = current_round(status)
    if round_number is None:
        raise LayoutError("No open quarantine records: nothing to do.")
    return round_number


def open_ids(status: pd.DataFrame, round_number: int) -> set[str]:
    mask = status["status"].eq(OPEN) & status["round"].eq(round_number)
    return set(status.loc[mask, "record_id"])


# ============================================================
# template
# ============================================================

def open_errors(layout: Layout, status: pd.DataFrame, round_number: int) -> pd.DataFrame:
    """Error lines of the records still open in a round."""

    errors = layout.load_errors(round_number)
    return errors[errors["record_id"].isin(open_ids(status, round_number))]


def build_template(errors: pd.DataFrame) -> pd.DataFrame:
    """One line per record and column, with every error on that field."""

    # Critical problems first; one line per record and column.
    errors = errors.assign(
        _order=errors["severity"].map({"critical": 0}).fillna(1),
        column=errors["column"].fillna(""),
    ).sort_values(["record_id", "_order"])

    def join(values: pd.Series) -> str:
        return " | ".join(dict.fromkeys(v for v in values.dropna()))

    template = (
        errors.groupby(["record_id", "column"], sort=False)
        .agg(
            current_value=("value", "first"),
            error_codes=("error_code", join),
            severity=("severity", "first"),
            messages=("message", join),
            rule_ids=("rule_id", join),
        )
        .reset_index()
    )
    template["new_value"] = ""
    template["action"] = ""
    template["reason"] = ""
    return template[TEMPLATE_COLUMNS]


def cmd_template(args: argparse.Namespace, registry: Registry) -> int:
    layout = open_layout(args, registry)
    status = load_status(layout)
    round_number = args.round or require_round(status)

    errors = open_errors(layout, status, round_number)
    if args.rule:
        errors = errors[errors["rule_id"].isin(args.rule)]
    if args.error_code:
        errors = errors[errors["error_code"].isin(args.error_code)]

    template = build_template(errors)
    output = args.output or Path(
        f"correction_template_{layout.run_id}_round_{round_number}.csv"
    )
    write_csv(template, output, excel=True)

    print(f"Round {round_number}: {template['record_id'].nunique()} records, "
          f"{len(template)} lines to review.")
    print(f"  template  {output}")
    return 0


# ============================================================
# import
# ============================================================

@dataclass
class ImportResult:
    round: int
    accepted: pd.DataFrame  # CORRECTION_COLUMNS
    refused: pd.DataFrame   # the refused lines, with the reason in 'refused'


def read_correction_file(source: Path | IO[bytes]) -> pd.DataFrame:
    """An edited template (path or uploaded file), every cell as text."""

    return pd.read_csv(
        source, dtype="string", keep_default_na=False, encoding="utf-8-sig",
    )


def import_corrections(
    layout: Layout,
    registry: Registry,
    lines: pd.DataFrame,
    user: str,
    dry_run: bool = False,
) -> ImportResult:
    """Check correction lines and store the accepted ones.

    lines needs record_id, column and new_value; action and reason are
    optional. Stores the round's corrections in dq.correction (the newest
    line per field wins), appends them to dq.correction_log and the refused
    lines to dq.import_rejection. dry_run only checks: nothing is written.
    """

    status = load_status(layout)
    round_number = require_round(status)
    allowed_ids = open_ids(status, round_number)

    missing = {"record_id", "column", "new_value"} - set(lines.columns)
    if missing:
        raise ConfigError(f"Corrections are missing columns {sorted(missing)}")

    lines = lines.copy()
    for column in ("action", "reason"):
        if column not in lines.columns:
            lines[column] = ""
    lines = lines[["record_id", "column", "new_value", "action", "reason"]]
    lines = lines.astype("string").fillna("").apply(lambda c: c.str.strip())
    lines["action"] = lines["action"].str.upper().replace("", "FIX")

    # Untouched template lines.
    touched = lines["new_value"].ne("") | lines["action"].isin(RECORD_ACTIONS)
    lines = lines[touched].copy()

    records = layout.load_quarantine(round_number).set_index("record_id")

    editable = {
        column for column, spec in registry.columns.items()
        if not spec.get("generated")
    }

    def refusal(row: pd.Series) -> str:
        if row["action"] not in ACTIONS:
            return f"unknown action '{row['action']}' (use FIX, DISCARD or ACCEPT)"
        if row["record_id"] not in allowed_ids:
            return f"record is not open in round {round_number}"
        if row["action"] in RECORD_ACTIONS:
            return ""
        if row["column"] not in editable:
            return f"'{row['column']}' is not an editable schema column"
        return ""

    lines["refused"] = lines.apply(refusal, axis=1) if len(lines) else ""

    # The same field corrected twice with different values: refuse both.
    fixes = lines["refused"].eq("") & lines["action"].eq("FIX")
    conflicting = (
        lines[fixes].groupby(["record_id", "column"])["new_value"]
        .transform("nunique").gt(1)
    )
    lines.loc[conflicting[conflicting].index, "refused"] = (
        "corrected more than once with different values"
    )

    accepted = lines[lines["refused"].eq("")].drop(columns="refused")
    accepted = accepted.drop_duplicates(["record_id", "column", "action"])
    refused = lines[lines["refused"].ne("")]

    def old_value(row: pd.Series) -> object:
        if row["action"] in RECORD_ACTIONS or row["column"] not in records.columns:
            return pd.NA
        return records.at[row["record_id"], row["column"]]

    timestamp = now_utc()
    accepted = accepted.assign(
        column=accepted["column"].where(accepted["action"].eq("FIX"), ""),
        old_value=[old_value(row) for _, row in accepted.iterrows()],
        user=user,
        corrected_at=timestamp,
    )[CORRECTION_COLUMNS]

    if dry_run:
        return ImportResult(round=round_number, accepted=accepted, refused=refused)

    with db.transaction():
        # Merge with earlier imports for this round; the newest line wins.
        existing = layout.load_corrections(round_number)
        existing["column"] = existing["column"].fillna("")
        merged = (
            pd.concat([existing, accepted], ignore_index=True)
            .drop_duplicates(["record_id", "column", "action"], keep="last")
        )
        layout.replace_corrections(round_number, merged)
        layout.append_log(round_number, accepted)
        layout.save_rejections(round_number, refused)

    return ImportResult(round=round_number, accepted=accepted, refused=refused)


def cmd_import(args: argparse.Namespace, registry: Registry) -> int:
    layout = open_layout(args, registry)
    result = import_corrections(
        layout, registry, read_correction_file(args.file),
        args.user or getpass.getuser(),
    )
    round_number, accepted, refused = result.round, result.accepted, result.refused

    print(f"Round {round_number}: {len(accepted)} corrections accepted "
          f"({int(accepted['action'].eq('DISCARD').sum())} discards), "
          f"{len(refused)} refused.")
    print("  corrections     dq.correction")
    print("  correction log  dq.correction_log")
    if len(refused):
        print("  refused lines   dq.import_rejection")
        for reason, count in refused["refused"].value_counts().items():
            print(f"    {count:>6}  {reason}")
    return 0


# ============================================================
# revalidate
# ============================================================

def _ids(values: set[str]) -> DataFrame:
    """A one-column DataFrame of record ids, for joins."""

    return F.broadcast(spark_io.local_frame(
        [(v,) for v in sorted(values)], "record_id string",
    ))


def _apply_fixes(records: DataFrame, fixes: pd.DataFrame, columns: list[str]) -> DataFrame:
    """The records with their FIX corrections applied (__NULL__ empties)."""

    if not len(fixes):
        return records
    fixed_columns = sorted(set(fixes["column"]))
    keys = {c: f"__fix{i}" for i, c in enumerate(fixed_columns)}
    lines = spark_io.local_frame(
        [(r.record_id, keys[r.column], r.new_value) for r in fixes.itertuples(index=False)],
        "record_id string, key string, new_value string",
    )
    wide = lines.groupBy("record_id").pivot("key", list(keys.values())).agg(F.first("new_value"))
    records = records.join(wide, "record_id", "left")
    return records.select(
        "record_id",
        *[
            F.when(col(keys[c]).isNull(), col(c))
            .when(col(keys[c]) == NULL_MARKER, F.lit(None).cast("string"))
            .otherwise(col(keys[c])).alias(c)
            if c in keys else col(c)
            for c in columns
        ],
        *[c for c in records.columns if c in ("dq_errors", "dq_warnings")],
    )


def revalidate(
    layout: Layout,
    registry: Registry,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    revalidate_all: bool = False,
    log: Callable[[str], None] = print,
) -> dict[str, Any] | None:
    """Apply the round's corrections and validate again (see module doc).

    Returns the round summary (also stored in dq.round_result), or None
    when there is nothing to re-validate.
    """

    clock = time.perf_counter()
    started = now_utc()
    status = load_status(layout).set_index("record_id")
    round_number = require_round(status.reset_index())
    next_round = round_number + 1
    pending = open_ids(status.reset_index(), round_number)

    corrections = layout.load_corrections(round_number)
    corrections = corrections[corrections["record_id"].isin(pending)]

    discard = set(corrections.loc[corrections["action"].eq("DISCARD"), "record_id"])
    fixes = corrections[
        corrections["action"].eq("FIX") & ~corrections["record_id"].isin(discard)
    ]
    # Accepted as they are: re-validated without a change. Their values are
    # already repaired, so the automatic-repair review does not fire again.
    accepted = set(corrections.loc[corrections["action"].eq("ACCEPT"), "record_id"]) - discard
    targets = (
        pending - discard if revalidate_all
        else set(fixes["record_id"]) | accepted
    )
    untouched = pending - targets - discard

    if not targets and not discard:
        return None

    columns = layout.columns()
    open_records = (
        f"record_id IN (SELECT record_id FROM dq.quarantine_status "
        f"WHERE dataset = {db.literal(layout.dataset)} "
        f"AND run_id = {db.literal(layout.run_id)} "
        f"AND status = 'OPEN' AND round = {round_number})"
    )
    records = layout.spark_records("quarantine", round_number, where=open_records)
    untouched_records = records.join(_ids(untouched), "record_id")

    # ---- Corrected records ----
    corrected = _apply_fixes(
        records.join(_ids(targets), "record_id"),
        fixes[fixes["record_id"].isin(targets)], columns,
    )

    # ---- Context: the rest of the dataset, for cross-row rules ----
    context = layout.spark_records("valid").select("record_id", *columns)
    if layout.has_records("resolved"):
        context = context.unionByName(
            layout.spark_records("resolved", where=f"round < {round_number}")
            .select("record_id", *columns)
        )
    context = context.unionByName(untouched_records.select("record_id", *columns))

    def framed(part: DataFrame, focus: bool, offset: int) -> DataFrame:
        return part.select(
            col("record_id").alias(RID),
            (F.monotonically_increasing_id() + offset).alias(ROW),
            F.lit(focus).alias(FOCUS),
            *[col(c) for c in columns],
        )

    frame = framed(context, False, 0).unionByName(
        framed(corrected, True, TARGET_ROW_OFFSET)
    )

    log(f"Round {round_number}: re-validating {len(targets)} corrected records "
        "against the rest of the dataset.")
    # Rule statistics count the corrected records only.
    result = validate_frame(frame, registry, log=log)

    outcomes = result.df.filter(col(FOCUS)).select(RID, ACCEPTED, QUARANTINED).collect()
    passed = {r[RID] for r in outcomes if r[ACCEPTED]}
    failed = {r[RID] for r in outcomes if r[QUARANTINED]}
    # A corrected record identical to another row is dropped as a duplicate.
    duplicates = targets - passed - failed

    # ---- Ledger ----
    timestamp = now_utc()
    attempts = status.loc[sorted(targets), "attempts"] + 1
    status.loc[sorted(targets), "attempts"] = attempts

    manual = {
        record for record in failed
        if status.at[record, "attempts"] >= max_attempts
    }
    still_open = (failed - manual) | untouched

    for records_set, new_status, new_round in (
        (passed, RESOLVED, round_number),
        (discard | duplicates, DISCARDED, round_number),
        (still_open, OPEN, next_round),
        (manual, MANUAL_REVIEW, next_round),
    ):
        if records_set:
            index = sorted(records_set)
            status.loc[index, "status"] = new_status
            status.loc[index, "round"] = new_round
            status.loc[index, "updated_at"] = timestamp

    failures_by_rule = [
        {"rule_id": r["rule_id"], "error_code": r["error_code"],
         "severity": r["severity"], "count": r["count"]}
        for r in result.errors(quarantined_only=True, focus=True)
        .groupBy("rule_id", "error_code", "severity").count()
        .orderBy("rule_id").collect()
    ]
    summary = {
        "run_id": layout.run_id,
        "dataset": layout.dataset,
        "round": next_round,
        "revalidated_round": round_number,
        "next_round": next_round,
        "started_at": started,
        "finished_at": timestamp,
        "duration_seconds": round(time.perf_counter() - clock, 2),
        "max_attempts": max_attempts,
        "counts": {
            "open_before": len(pending),
            "revalidated": len(targets),
            "resolved": len(passed),
            "failed_again": len(failed - manual),
            "manual_review_required": len(manual),
            "discarded": len(discard),
            "removed_as_duplicate": len(duplicates),
            "carried_over_untouched": len(untouched),
            "open_after": len(still_open),
        },
        "failures_by_rule": failures_by_rule,
        "status_totals": status["status"].value_counts().to_dict(),
        "rules": result.rule_stats,
        "outputs": {
            "resolved": ".".join(db.record_table_name(layout.dataset, "resolved")),
            "next_round": ".".join(db.record_table_name(layout.dataset, "quarantine")),
            "result": "dq.round_result",
        },
    }

    # ---- Outputs: records first, the ledger and round result last ----
    stamp = {"dataset": layout.dataset, "run_id": layout.run_id, "round": next_round}

    def stamped(df: DataFrame, constants: dict[str, Any]) -> DataFrame:
        return df.select(*[F.lit(v).alias(k) for k, v in constants.items()], "*")

    resolved_table = layout.prepare_records("resolved", round_number)
    next_table = layout.prepare_records("quarantine", next_round)
    layout.clear(ERRORS, next_round)
    layout.clear(AUDIT, next_round)

    spark_io.write(
        stamped(result.valid(focus=True), {"run_id": layout.run_id, "round": round_number}),
        resolved_table,
    )
    next_records = result.quarantine(focus=True).unionByName(
        untouched_records.select("record_id", *columns, "dq_errors", "dq_warnings")
    )
    spark_io.write(stamped(next_records, {"run_id": layout.run_id, "round": next_round}),
                   next_table)
    next_errors = result.errors(quarantined_only=True, focus=True).unionByName(
        layout.spark_table("dq.quarantine_error", ERROR_COLUMNS, round_number)
        .join(_ids(untouched), "record_id")
    )
    spark_io.write(stamped(next_errors, stamp), "dq.quarantine_error")
    spark_io.write(stamped(result.audit(focus=True), stamp), "dq.remediation_audit")
    result.release()

    with db.transaction():
        save_status(layout, status.reset_index())
        layout.save_result(next_round, summary)

    return summary


def cmd_revalidate(args: argparse.Namespace, registry: Registry) -> int:
    layout = open_layout(args, registry)
    summary = revalidate(layout, registry, args.max_attempts, args.all)

    if summary is None:
        print("No corrections to re-validate. Import a correction file "
              "first (or use --all).")
        return 0

    counts = summary["counts"]
    print(
        f"\nResolved {counts['resolved']}, failed again "
        f"{counts['failed_again']}, manual review "
        f"{counts['manual_review_required']}, discarded "
        f"{counts['discarded'] + counts['removed_as_duplicate']}, "
        f"carried over {counts['carried_over_untouched']}."
    )
    for key, table in summary["outputs"].items():
        print(f"  {key:<17} {table}")
    return 0


# ============================================================
# status
# ============================================================

def cmd_status(args: argparse.Namespace, registry: Registry) -> int:
    layout = open_layout(args, registry)
    status = load_status(layout)

    print(f"Run {layout.run_id} ({layout.dataset})")
    table = (
        status.groupby(["status", "round"]).size()
        .rename("records").reset_index()
    )
    print(table.to_string(index=False) if len(table) else "  no quarantined records")

    round_number = current_round(status)
    print(f"Current round: {round_number if round_number else 'none open'}")
    return 0


# ============================================================
# Main
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Correct and re-validate quarantined records.",
    )
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--schema", type=Path, default=None)

    commands = parser.add_subparsers(dest="command", required=True)

    def add(name: str, help_text: str) -> argparse.ArgumentParser:
        command = commands.add_parser(name, help=help_text)
        command.add_argument(
            "--run", default="latest",
            help="Run (ingestion) id, or 'latest' (default).",
        )
        return command

    template = add("template", "Write the correction template.")
    template.add_argument("--round", type=int, default=None,
                          help="Round (default: the current one).")
    template.add_argument("--rule", nargs="*", default=None,
                          help="Only errors from these rule ids.")
    template.add_argument("--error-code", nargs="*", default=None,
                          help="Only errors with these codes (e.g. DQ-NULL).")
    template.add_argument("--output", type=Path, default=None)

    importer = add("import", "Import an edited correction template.")
    importer.add_argument("--file", type=Path, required=True)
    importer.add_argument("--user", default=None,
                          help="Who made the corrections (default: OS user).")

    revalidate = add("revalidate", "Apply corrections and validate again.")
    revalidate.add_argument("--max-attempts", type=int,
                            default=DEFAULT_MAX_ATTEMPTS,
                            help="Failed re-validations before a record "
                            "needs manual review.")
    revalidate.add_argument("--all", action="store_true",
                            help="Re-validate every open record, also those "
                            "without corrections (e.g. after a rule change).")

    add("status", "Show quarantine counts.")

    return parser.parse_args()


COMMANDS = {
    "template": cmd_template,
    "import": cmd_import,
    "revalidate": cmd_revalidate,
    "status": cmd_status,
}


def main() -> int:
    args = parse_args()
    registry = load_registry(args.rules, args.schema)
    return COMMANDS[args.command](args, registry)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ConfigError, LayoutError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
    finally:
        spark_io.stop()
