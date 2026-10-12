"""Merge into silver: valid records plus the records resolved in quarantine (Spark).

Combines the run's valid records (dq.<dataset>_valid) with the records
resolved in every round (dq.<dataset>_resolved). Rows are matched on record
id and a resolved row replaces any earlier version of the same record, so
each record appears once. Records still open, waiting for manual review or
discarded are left out.

    silver.<dataset>    the clean data, typed: one row per record (the
                        run's rows are replaced on every merge)
    dq.merge_result     counts and checks of the run's latest merge

Silver columns get the type of the schema registry: integer -> bigint,
decimal -> numeric, boolean -> boolean, date -> date, time -> time, other
types and columns outside the registry -> text. Accepted records already
hold valid values (invalid ones were nulled on acceptance); any value that
still cannot be converted is stored as null and counted per column in
dq.merge_result ("type_conversion_nulls").

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
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F

import db
from dq_engine import DEFAULT_RULES
from layout import Layout, LayoutError, latest_run, now_utc
from registry import ConfigError, Registry, load_registry
from values import format_number, format_temporal, ident, lit, parse_values

spark_io = db.spark_io

PG_TYPES = {
    "integer": "bigint",
    "decimal": "numeric",
    "boolean": "boolean",
    "date": "date",
    "time": "time",
}


def silver_types(registry: Registry, columns: list[str]) -> dict[str, str]:
    """PostgreSQL type of every silver column."""

    return {
        c: PG_TYPES.get(registry.columns.get(c, {}).get("type", "string"), "text")
        for c in columns
    }


def typed_value(registry: Registry, column: str) -> str:
    """The value as silver stores it (SQL): a Spark type, or canonical text
    that PostgreSQL converts (dates, times, numerics). Null when it does
    not convert."""

    spec = registry.columns.get(column, {"type": "string"})
    kind = spec.get("type", "string")
    value = parse_values(ident(column), spec)
    if kind == "integer":
        return f"CAST({value} AS BIGINT)"
    if kind == "decimal":
        return format_number(value)
    if kind == "boolean":
        return value
    if kind == "date":
        return format_temporal(value, "%Y-%m-%d")
    if kind == "time":
        return format_temporal(value, "%H:%M:%S")
    return ident(column)


def merge_run(layout: Layout, registry: Registry, published_by: str | None = None) -> dict:
    """Replace the run's rows in silver.<dataset>; return the merge summary."""

    columns = layout.columns()
    valid = layout.spark_records("valid").persist(StorageLevel.MEMORY_AND_DISK)

    if layout.has_records("resolved"):
        resolved_rounds = layout.spark_records("resolved", with_round=True)
    else:
        resolved_rounds = valid.limit(0).withColumn("round", F.lit(0))
    resolved_rounds = resolved_rounds.persist(StorageLevel.MEMORY_AND_DISK)

    # A record resolved in more than one round: the latest version wins.
    latest = Window.partitionBy("record_id").orderBy(F.col("round").desc())
    resolved = (
        resolved_rounds.withColumn("__n", F.row_number().over(latest))
        .filter(F.col("__n") == 1).drop("__n", "round")
    )

    ledger = spark_io.read(
        "SELECT record_id FROM dq.quarantine_status "
        f"WHERE dataset = {db.literal(layout.dataset)} "
        f"AND run_id = {db.literal(layout.run_id)} AND status = 'RESOLVED'"
    )
    not_in_ledger = resolved.select("record_id").join(ledger, "record_id", "left_anti")
    missing = not_in_ledger.limit(3).collect()
    if missing:
        raise ConfigError(
            f"{not_in_ledger.count()} resolved records are not RESOLVED in the "
            f"ledger, e.g. {[r['record_id'] for r in missing]}"
        )

    final = (
        valid.join(resolved.select("record_id"), "record_id", "left_anti")
        .unionByName(resolved)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    valid_rows = valid.count()
    resolved_rows = resolved.count()
    replaced = valid.join(resolved.select("record_id"), "record_id").count()
    final_rows = final.count()
    expected = valid_rows + resolved_rows - replaced
    if final_rows != expected:
        raise ConfigError(
            f"Merged {final_rows} rows, expected valid {valid_rows} + "
            f"resolved {resolved_rows} - replaced {replaced} = {expected}."
        )
    if final.groupBy("record_id").count().filter("count > 1").limit(1).count():
        raise ConfigError("Merged data has duplicate record ids.")

    # ---- Typed silver ----
    types = silver_types(registry, columns)
    typed = final.selectExpr(
        f"{lit(layout.run_id)} AS run_id", "record_id",
        *[f"{typed_value(registry, c)} AS {ident(c)}" for c in columns], "dq_warnings",
    )
    checked = [c for c, kind in types.items() if kind != "text"]
    conversions = final.selectExpr(*[
        f"sum(CASE WHEN {ident(c)} IS NOT NULL AND ({typed_value(registry, c)}) IS NULL "
        f"THEN 1 ELSE 0 END) AS {ident(c)}"
        for c in checked
    ]).collect()[0].asDict() if checked else {}

    spark_io.write(typed, layout.prepare_silver(types))

    status = {r["status"]: r["n"] for r in spark_io.read(
        "SELECT status, count(*) AS n FROM dq.quarantine_status "
        f"WHERE dataset = {db.literal(layout.dataset)} "
        f"AND run_id = {db.literal(layout.run_id)} GROUP BY status"
    ).collect()}
    by_round = {
        str(r["round"]): r["n"] for r in resolved_rounds.groupBy("round")
        .agg(F.countDistinct("record_id").alias("n")).orderBy("round").collect()
    }
    for frame in (valid, resolved_rounds, final):
        frame.unpersist()

    summary: dict[str, Any] = {
        "run_id": layout.run_id,
        "dataset": layout.dataset,
        "merged_at": now_utc(),
        "published_by": published_by,
        "counts": {
            "valid": valid_rows,
            "resolved": resolved_rows,
            "replaced": replaced,
            "final_rows": final_rows,
            "left_out_open": int(status.get("OPEN", 0)),
            "left_out_manual_review": int(status.get("MANUAL_REVIEW_REQUIRED", 0)),
            "left_out_discarded": int(status.get("DISCARDED", 0)),
        },
        "resolved_by_round": by_round,
        "checks": {
            "rows_equal_valid_plus_resolved": final_rows == valid_rows + resolved_rows,
            "unique_record_ids": True,
        },
        "column_types": types,
        "type_conversion_nulls": {c: int(n) for c, n in conversions.items() if n},
        "outputs": {"clean": ".".join(db.record_table_name(layout.dataset, "silver"))},
    }
    layout.save_merge_result(summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge valid and resolved records into silver.<dataset>.",
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

    summary = merge_run(Layout(dataset, run_id), registry)
    counts = summary["counts"]

    print(f"Run {run_id}: {counts['final_rows']} rows = valid "
          f"{counts['valid']} + resolved {counts['resolved']}"
          + (f" - replaced {counts['replaced']}" if counts["replaced"] else "")
          + ".")
    print(f"  left out: open {counts['left_out_open']}, manual review "
          f"{counts['left_out_manual_review']}, discarded "
          f"{counts['left_out_discarded']}")
    if summary["type_conversion_nulls"]:
        print(f"  values that did not convert to their type (stored as null): "
              f"{summary['type_conversion_nulls']}")
    print(f"  clean  {summary['outputs']['clean']}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ConfigError, LayoutError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
    finally:
        spark_io.stop()
