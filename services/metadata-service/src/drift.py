"""File-to-file drift check: compare the profile of an ingestion with the
previous one (both stored in bronze.ingestion.profile) and alert on big changes.

Checks: row count, missing-value rate per column, new category values,
mean of numeric columns. Thresholds: config/drift/drift_rules.yaml.
Results: dq.drift_check / dq.drift_finding. Alert-only: exit code 0 even
when drift is found (the pipeline continues).

    python src/drift.py --ingestion latest
    python src/drift.py --ingestion ING_20261111_003 --against ING_20261111_001
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "services" / "common" / "src"))
sys.path.append(str(ROOT / "airflow" / "dags"))      # dq_notify (e-mail)
import bronze  # noqa: E402
import schemas  # noqa: E402

DEFAULT_RULES = ROOT / "config" / "drift" / "drift_rules.yaml"
DEFAULT_DATASET = "banking_transactions"


def load_rules(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def finding(check, severity, message, column=None, old=None, new=None, change=None):
    return {"check_name": check, "severity": severity, "message": message,
            "column_name": column, "old_value": old, "new_value": new, "change": change}


def by_name(profile: dict) -> dict[str, dict]:
    return {c["column_name"]: c for c in profile["columns"]}


# ---------------- the four checks ----------------

# checks for row count that is too low or too high than the previous file   
def check_row_count(old, new, cfg):
    before, after = old.get("row_count"), new.get("row_count")
    if not before:
        return []
    change = (after - before) / before
    text = f"({before:,} -> {after:,} rows)"
    # 
    if change <= -cfg.get("drop_pct", 0.5):
        return [finding("row_count", "critical", f"Row count fell {abs(change):.0%} {text}",
                        None, before, after, change)]
    if change >= cfg.get("rise_pct", 1.0):
        return [finding("row_count", "warning", f"Row count grew {change:.0%} {text}",
                        None, before, after, change)]
    return []

# checks for missing-value rate that is too high or rising too fast than the previous file
def check_nulls(old, new, cfg):
    before, out = by_name(old), []
    for c in new["columns"]:
        o = before.get(c["column_name"])
        if o is None:
            continue
        delta = c["missing_percentage"] - o["missing_percentage"]
        severity = ("critical" if delta >= cfg.get("critical_pp", 20)
                    else "warning" if delta >= cfg.get("warning_pp", 5) else None)
        if severity:
            out.append(finding(
                "null_rate", severity,
                f"{c['column_name']}: missing values rose from "
                f"{o['missing_percentage']:.1f}% to {c['missing_percentage']:.1f}% ({delta:+.1f} pp)",
                c["column_name"], o["missing_percentage"], c["missing_percentage"], delta))
    return out

# checks for new category values in categorical columns that are not in the previous file
def check_categories(old, new, cfg):
    levels = {**{c: "warning" for c in cfg.get("warning", [])},
              **{c: "critical" for c in cfg.get("critical", [])}}
    before, out = by_name(old), []
    for c in new["columns"]:
        name, o = c["column_name"], before.get(c["column_name"])
        if name not in levels or o is None:
            continue
        if "category_values" not in c or "category_values" not in o:
            continue                                  # profile without categories
        added = sorted(set(c["category_values"]) - set(o["category_values"]))
        if added:
            shown = ", ".join(repr(v) for v in added[:5]) + (" ..." if len(added) > 5 else "")
            out.append(finding("new_category", levels[name],
                               f"{name}: new value(s) not in the previous file: {shown}",
                               name, None, shown, len(added)))
    return out

# checks for mean of numeric columns that changed too much from the previous file
def check_means(old, new, cfg):
    before, after, out = by_name(old), by_name(new), []
    for name in cfg.get("columns", []):
        o, n = before.get(name), after.get(name)
        if not o or not n or not o.get("mean") or n.get("mean") is None:
            continue
        change = (n["mean"] - o["mean"]) / abs(o["mean"])
        severity = ("critical" if abs(change) >= cfg.get("critical_pct", 1.0)
                    else "warning" if abs(change) >= cfg.get("warning_pct", 0.5) else None)
        if severity:
            out.append(finding("mean_change", severity,
                               f"{name}: average went from {o['mean']:,.2f} to {n['mean']:,.2f} ({change:+.0%})",
                               name, o["mean"], n["mean"], change))
    return out

# compare two profiles and return a list of findings (dicts) for each check that failed
def compare(old: dict, new: dict, rules: dict) -> list[dict]:
    return (check_row_count(old, new, rules.get("row_count", {}))
            + check_nulls(old, new, rules.get("null_rate", {}))
            + check_categories(old, new, rules.get("new_categories", {}))
            + check_means(old, new, rules.get("mean_change", {})))

# function to determine the overall severity of the findings
def overall(findings: list[dict]) -> str:
    severities = {f["severity"] for f in findings}
    return "CRITICAL" if "critical" in severities else "WARNING" if severities else "OK"


# ---------------- database, e-mail ----------------
# function to get the previous ingestion_id for a given dataset and ingestion_id so that we can compare the two profiles
def previous_id(conn, dataset: str, ingestion_id: str) -> str | None:
    row = conn.execute(
        "SELECT ingestion_id FROM bronze.ingestion WHERE dataset = %s AND ingestion_id < %s "
        "AND profile IS NOT NULL ORDER BY ingestion_id DESC LIMIT 1",
        (dataset, ingestion_id)).fetchone()
    return row[0] if row else None

# store in the database the drift check results in (dq.drift_check) & findings in (dq.drift_finding)
def store(conn, dataset, run_id, previous, status, findings, emailed) -> None:
    with conn.transaction():
        conn.execute("DELETE FROM dq.drift_check WHERE dataset = %s AND run_id = %s",
                     (dataset, run_id))                     # findings cascade
        conn.execute(
            "INSERT INTO dq.drift_check (dataset, run_id, previous_run_id, status, "
            "finding_count, emailed) VALUES (%s, %s, %s, %s, %s, %s)",
            (dataset, run_id, previous, status, len(findings), emailed))
        for f in findings:
            conn.execute(
                "INSERT INTO dq.drift_finding (dataset, run_id, previous_run_id, check_name, "
                "column_name, severity, old_value, new_value, change, message) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (dataset, run_id, previous, f["check_name"], f["column_name"], f["severity"],
                 None if f["old_value"] is None else str(f["old_value"]),
                 None if f["new_value"] is None else str(f["new_value"]),
                 f["change"], f["message"]))

# function to send an e-mail with the drift check results
def send_email(current, previous, status, findings) -> bool:
    try:
        import dq_notify
    except ImportError:
        print("dq_notify not found: no e-mail sent.")
        return False
    lines = [f"Data drift ({status}) between two files of {current['dataset']}.", "",
             f"New file:      {current['source_file']} ({current['ingestion_id']})",
             f"Previous file: {previous['source_file']} ({previous['ingestion_id']})", "",
             "Severity   Check          Column                  Detail", "-" * 78]
    for f in sorted(findings, key=lambda f: f["severity"] != "critical"):
        lines.append(f"{f['severity'].upper():<10} {f['check_name']:<14} "
                     f"{(f['column_name'] or '-'):<23} {f['message']}")
    lines += ["", "The pipeline continued. Check the source before trusting this load."]
    return dq_notify.send(f"{dq_notify.PREFIX} DATA DRIFT ({status}): {current['source_file']}",
                          "\n".join(lines))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compare an ingestion's profile with the previous one.")
    p.add_argument("--ingestion", default="latest")
    p.add_argument("--against", default=None, help="Ingestion to compare with (default: the previous one).")
    p.add_argument("--dataset-name", default=DEFAULT_DATASET)
    p.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    p.add_argument("--no-email", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    rules = load_rules(args.rules)
    dataset = args.dataset_name

    with bronze.connect() as conn:
        schemas.ensure(conn, "06_drift.sql")
        current = bronze.find_ingestion(conn, dataset, args.ingestion)
        if not current["profile"]:
            print(f"Error: {current['ingestion_id']} has no profile; run the profile step first.",
                  file=sys.stderr)
            return 2

        prev_id = args.against or previous_id(conn, dataset, current["ingestion_id"])
        if prev_id is None:
            # no previous ingestion to compare with, store the current ingestion as the baseline
            store(conn, dataset, current["ingestion_id"], None, "BASELINE", [], False)
            print(f"{current['ingestion_id']}: first file of {dataset}, stored as the baseline.")
            return 0
        previous = bronze.find_ingestion(conn, dataset, prev_id)
        if not previous["profile"]:
            print(f"Error: {prev_id} has no profile.", file=sys.stderr)
            return 2

        findings = compare(previous["profile"], current["profile"], rules)
        status = overall(findings)
        print(f"{current['ingestion_id']} ({current['source_file']}) vs "
              f"{previous['ingestion_id']} ({previous['source_file']}): {status}")
        for f in findings:
            print(f"  [{f['severity']:<8}] {f['message']}")

        emailed = bool(findings) and not args.no_email and send_email(current, previous, status, findings)
        store(conn, dataset, current["ingestion_id"], previous["ingestion_id"], status, findings, emailed)
        if findings:
            print("E-mail sent." if emailed else "E-mail not sent.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except bronze.BronzeError as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)