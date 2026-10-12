"""Load runs kept in data/ by earlier versions into the database (once).

Earlier versions wrote every output of a run to files under data/:

    validated/<dataset>/<run>/valid.csv
    quarantine/<dataset>/<run>/quarantine_status.csv, quarantine_history.csv,
                               round_<n>/quarantine_records.csv,
                               round_<n>/quarantine_errors.csv
    corrected/<dataset>/<run>/round_<n>/corrections.csv, resolved.csv,
                                        import_rejections.csv
    quality/<dataset>/<run>/correction_log.csv,
                            round_<n>/dq_result.json, remediation_audit.csv
    final/<dataset>/<run>/clean.csv, merge_result.json

This stores each such run in the dq tables (layout.py) as if it had been
run against the database, keeping its rounds, ledger history and
correction log. Its clean data is rebuilt, typed, by running the merge
afterwards (python dq.py merge --run <id>). Runs already in the database
are left alone. The files are not changed; delete data/ yourself once the
import is checked.

Usage
-----
    python src/import_files.py                       # every run
    python src/import_files.py --run ING_20261011_001
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

import db
from dq_engine import DEFAULT_RULES, PROJECT_ROOT
from layout import (
    CORRECTION_LOG,
    ERROR_COLUMNS,
    HISTORY,
    HISTORY_COLUMNS,
    LOG_COLUMNS,
    STATUS,
    STATUS_COLUMNS,
    Layout,
    LayoutError,
    read_text_csv,
)
from registry import ConfigError, load_registry


def rounds(folder: Path, file_name: str) -> list[tuple[int, Path]]:
    """(round, path) of round_<n>/<file_name> under folder, by round."""

    found = [
        (int(path.parent.name.removeprefix("round_")), path)
        for path in folder.glob(f"round_*/{file_name}")
    ]
    return sorted(found)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def import_run(data: Path, dataset: str, run_id: str) -> str:
    """Store one run; returns what was done."""

    layout = Layout(dataset, run_id)
    if layout.exists():
        return "already in the database, skipped"

    def area(name: str) -> Path:
        return data / name / dataset / run_id

    valid_file = area("validated") / "valid.csv"
    if not valid_file.is_file():
        return f"no {valid_file}, skipped"

    valid = read_text_csv(valid_file)
    columns = [c for c in valid.columns if c not in ("record_id", "dq_warnings")]
    results = {n: read_json(path) for n, path in rounds(area("quality"), "dq_result.json")}
    source = (results.get(1) or {}).get("source_file") or str(valid_file)

    with db.transaction() as conn:
        layout.start(columns, source)
        layout.save_valid(valid)

        for n, path in rounds(area("quarantine"), "quarantine_records.csv"):
            errors_file = path.parent / "quarantine_errors.csv"
            errors = (
                read_text_csv(errors_file) if errors_file.is_file()
                else pd.DataFrame(columns=ERROR_COLUMNS)
            )
            layout.save_quarantine(n, read_text_csv(path), errors)

        for n, path in rounds(area("quality"), "remediation_audit.csv"):
            layout.save_audit(n, read_text_csv(path))
        for n, result in results.items():
            layout.save_result(n, result)

        for n, path in rounds(area("corrected"), "corrections.csv"):
            layout.replace_corrections(n, read_text_csv(path))
        for n, path in rounds(area("corrected"), "resolved.csv"):
            layout.save_resolved(n, read_text_csv(path))
        for n, path in rounds(area("corrected"), "import_rejections.csv"):
            layout.save_rejections(n, read_text_csv(path))

        log_file = area("quality") / "correction_log.csv"
        if log_file.is_file():
            db.write_frame(conn, CORRECTION_LOG,
                           read_text_csv(log_file)[LOG_COLUMNS], fixed=layout.key)

        # The ledger and its history as they were (save_status would date
        # every entry to now).
        status = read_text_csv(area("quarantine") / "quarantine_status.csv")
        history_file = area("quarantine") / "quarantine_history.csv"
        history = (
            read_text_csv(history_file) if history_file.is_file()
            else status.rename(columns={"updated_at": "changed_at"})
        )
        db.write_frame(conn, STATUS, status[STATUS_COLUMNS], fixed=layout.key)
        db.write_frame(conn, HISTORY, history[HISTORY_COLUMNS], fixed=layout.key)

    return (f"imported: {len(valid)} valid, {len(status)} quarantined, "
            f"{len(results)} rounds")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load runs kept in data/ by earlier versions into the database.",
    )
    parser.add_argument("--run", default="all",
                        help="Run (ingestion) id, or 'all' (default).")
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--schema", type=Path, default=None)
    parser.add_argument("--data", type=Path, default=PROJECT_ROOT / "data",
                        help="Folder holding validated/, quarantine/, ...")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_registry(args.rules, args.schema)
    dataset = registry.dataset.get("name", "dataset")

    runs = sorted(
        folder.parent.name
        for folder in (args.data / "quarantine" / dataset).glob("*/quarantine_status.csv")
    )
    if args.run != "all":
        runs = [r for r in runs if r == args.run]
    if not runs:
        print(f"No runs of {dataset} under {args.data}.")
        return 0

    for run_id in runs:
        print(f"{run_id}: {import_run(args.data, dataset, run_id)}", flush=True)
    print("Then for each run: python dq.py merge --run <id>; and once: python dq.py metrics")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ConfigError, LayoutError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
