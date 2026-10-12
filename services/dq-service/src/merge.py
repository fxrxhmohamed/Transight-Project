"""Merge into silver: valid records plus the records resolved in quarantine.

Combines the run's valid records (dq.<dataset>_valid) with the records
resolved in every round (dq.<dataset>_resolved). Rows are matched on record
id and a resolved row replaces any earlier version of the same record, so
each record appears once. Records still open, waiting for manual review or
discarded are left out.

    silver.<dataset>    the clean data: one row per record (the run's rows
                        are replaced on every merge)
    dq.merge_result     counts and checks of the run's latest merge

Checks (the merge fails if one does not hold):
    rows = valid + resolved - replaced
    no record id appears twice
    every resolved record is RESOLVED in the quarantine ledger

Usage
-----
    python src/merge.py --run ING_20261011_001
    python src/merge.py                       # latest run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

import db
from dq_engine import DEFAULT_RULES
from layout import (
    RESOLVED,
    Layout,
    LayoutError,
    latest_run,
    load_status,
    now_utc,
)
from registry import ConfigError, load_registry


def merge_run(layout: Layout, published_by: str | None = None) -> dict:
    """Replace the run's rows in silver.<dataset>; return the merge summary."""

    valid = layout.load_valid()
    status = load_status(layout)
    columns = list(valid.columns)

    parts = layout.load_resolved()
    resolved = (
        pd.concat(
            [frame.assign(_round=n) for n, frame in parts], ignore_index=True,
        )
        if parts else pd.DataFrame(columns=[*columns, "_round"])
    )

    unknown = set(resolved.columns) - set(columns) - {"_round"}
    if unknown:
        raise ConfigError(
            f"Resolved records have columns the valid records lack: {sorted(unknown)}"
        )

    # A record resolved in more than one round: the latest version wins.
    resolved = (
        resolved.sort_values("_round", kind="stable")
        .drop_duplicates("record_id", keep="last")
        .reindex(columns=columns)
    )

    resolved_ids = set(status.loc[status["status"].eq(RESOLVED), "record_id"])
    not_in_ledger = set(resolved["record_id"]) - resolved_ids
    if not_in_ledger:
        raise ConfigError(
            f"{len(not_in_ledger)} resolved records are not RESOLVED in the "
            f"ledger, e.g. {sorted(not_in_ledger)[:3]}"
        )

    replaced = int(valid["record_id"].isin(resolved["record_id"]).sum())
    final = (
        pd.concat([valid, resolved], ignore_index=True)
        .drop_duplicates("record_id", keep="last")
        .sort_values("record_id", kind="stable", ignore_index=True)
    )

    expected = len(valid) + len(resolved) - replaced
    if len(final) != expected:
        raise ConfigError(
            f"Merged {len(final)} rows, expected valid {len(valid)} + "
            f"resolved {len(resolved)} - replaced {replaced} = {expected}."
        )
    if final["record_id"].duplicated().any():
        raise ConfigError("Merged data has duplicate record ids.")

    counts = status["status"].value_counts()
    summary = {
        "run_id": layout.run_id,
        "dataset": layout.dataset,
        "merged_at": now_utc(),
        "published_by": published_by,
        "counts": {
            "valid": len(valid),
            "resolved": len(resolved),
            "replaced": replaced,
            "final_rows": len(final),
            "left_out_open": int(counts.get("OPEN", 0)),
            "left_out_manual_review": int(counts.get("MANUAL_REVIEW_REQUIRED", 0)),
            "left_out_discarded": int(counts.get("DISCARDED", 0)),
        },
        "resolved_by_round": {
            str(n): int(frame["record_id"].nunique()) for n, frame in parts
        },
        "checks": {
            "rows_equal_valid_plus_resolved": len(final) == len(valid) + len(resolved),
            "unique_record_ids": True,
        },
        "outputs": {"clean": ".".join(db.record_table_name(layout.dataset, "silver"))},
    }

    layout.save_final(final, summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge valid and resolved records into the final dataset.",
    )
    parser.add_argument("--run", default="latest",
                        help="Run (ingestion) id, or 'latest' (default).")
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--schema", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_registry(args.rules, args.schema)
    dataset = registry.dataset.get("name", "dataset")
    run_id = latest_run(dataset) if args.run == "latest" else args.run

    summary = merge_run(Layout(dataset, run_id))
    counts = summary["counts"]

    print(f"Run {run_id}: {counts['final_rows']} rows = valid "
          f"{counts['valid']} + resolved {counts['resolved']}"
          + (f" - replaced {counts['replaced']}" if counts["replaced"] else "")
          + ".")
    print(f"  left out: open {counts['left_out_open']}, manual review "
          f"{counts['left_out_manual_review']}, discarded "
          f"{counts['left_out_discarded']}")
    print(f"  clean  {summary['outputs']['clean']}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ConfigError, LayoutError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
