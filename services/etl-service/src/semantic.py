"""Semantic layer: compute the dbt/MetricFlow metrics and store them in PostgreSQL.

    1. dbt build (dbt/): the views MetricFlow reads (semantic.fct_transactions,
       dim_customers, dim_branches, dim_banks over gold; dq_runs and
       dq_rule_checks over the DQ tables), the time spine, their tests;
       not the DQ tables themselves (tag dq: the metrics step builds them)
    2. mf query: every metric of dbt/models/semantic/_metrics.yml per month
       and in total, and the data-quality metrics per run too
    3. one transaction: semantic.metric_definition (label, description,
       type, formula, unit, area, headline) and semantic.metric_value
       (sql/05_semantic.sql, applied again first) replaced with what was
       computed

So the business and data-quality metrics are defined once (in dbt) and
readable by anyone with the database: Power BI reads
semantic.metric_report, or the gold tables with the matching DAX measures
(semantic/). The DQ tables must exist: run "python dq.py metrics" first.

dbt and mf run as dbt_project.py says (services/common/src).

Usage
-----
    python src/semantic.py
"""

from __future__ import annotations

import csv
import json
import math
import re
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "services" / "common" / "src"))
import bronze  # noqa: E402
import dbt_project  # noqa: E402
from dbt_project import DbtError as SemanticError, executables  # noqa: E402

SQL_FILE = ROOT / "sql" / "05_semantic.sql"
DQ_AREA = "Data Quality"   # metrics also stored per run
RUN = "dq_run__run_id"
# The copy of dbt/ dbt and mf run in (set by main).
project = dbt_project.DBT_PROJECT
TOTAL_PERIOD = date(1, 1, 1)
DECIMALS = 6


def call(*line: str) -> None:
    dbt_project.call(project, *line)


# ============================================================
# Definitions (from dbt's manifest)
# ============================================================

def _name(ref: Any) -> str:
    return ref.get("name") if isinstance(ref, dict) else str(ref)


def _filter(node: dict[str, Any]) -> str:
    """The filter in plain SQL terms: {{ Dimension('transaction__x') }} -> x."""

    where = (node.get("filter") or {}).get("where_filters") or []
    text = " AND ".join(w.get("where_sql_template", "") for w in where)
    return re.sub(r"\{\{\s*\w+\('(?:\w+__)?(\w+)'\)\s*\}\}", r"\1", text)


def measures(manifest: dict[str, Any]) -> dict[str, str]:
    """Measure -> its aggregation over the model, e.g. count_distinct(customer_id)."""

    found = {}
    for model in manifest.get("semantic_models", {}).values():
        for measure in model.get("measures") or []:
            found[measure["name"]] = f"{measure['agg']}({measure.get('expr') or measure['name']})"
    return found


def formula(metric: dict[str, Any], aggregations: dict[str, str]) -> str:
    """The metric in one line: measure, ratio or expression, with its filter."""

    params = metric.get("type_params") or {}
    kind = metric.get("type")
    if kind == "simple":
        measure = _name(params.get("measure"))
        text = aggregations.get(measure, measure)
    elif kind == "ratio":
        text = f"{_name(params.get('numerator'))} / {_name(params.get('denominator'))}"
    elif kind == "derived":
        inputs = []
        for item in params.get("metrics") or []:
            offset = item.get("offset_window")
            note = f" {offset['count']} {offset['granularity']} earlier" if offset else ""
            alias = f" as {item['alias']}" if item.get("alias") else ""
            inputs.append(f"{item['name']}{note}{alias}")
        text = f"{params.get('expr')} [{', '.join(inputs)}]"
    else:
        text = json.dumps(params, default=str)
    where = _filter(metric)
    return f"{text} where {where}" if where else text


def has_offset(metric: dict[str, Any]) -> bool:
    """Needs a time grain: an offset (prior year, month) or cumulative (YTD,
    rolling) metric; stored per month only."""

    if metric.get("type") == "cumulative":
        return True
    return any(item.get("offset_window") or item.get("offset_to_grain")
               for item in (metric.get("type_params") or {}).get("metrics") or [])


def definitions() -> list[dict[str, Any]]:
    manifest = json.loads((project / "target" / "manifest.json").read_text(encoding="utf-8"))
    metrics = manifest.get("metrics", {})
    aggregations = measures(manifest)
    rows = []
    for metric in sorted(metrics.values(), key=lambda m: m["name"]):
        meta = (metric.get("config") or {}).get("meta") or metric.get("meta") or {}
        rows.append({
            "metric": metric["name"],
            "label": metric.get("label") or metric["name"],
            "description": " ".join((metric.get("description") or "").split()) or None,
            "metric_type": metric.get("type"),
            "formula": formula(metric, aggregations),
            "unit": meta.get("unit"),
            "area": meta.get("area"),
            "headline": bool(meta.get("headline")),
            "offset": has_offset(metric),
            "by_run": meta.get("area") == DQ_AREA,
        })
    if not rows:
        raise SemanticError("dbt's manifest has no metrics.")
    return rows


# ============================================================
# Values (from MetricFlow)
# ============================================================

def number(text: str | None) -> float | None:
    if text is None or text.strip().lower() in ("", "none", "nan", "null"):
        return None
    value = float(text)
    return None if math.isnan(value) or math.isinf(value) else value


def query(mf: str, metrics: list[str], folder: Path, grain: str) -> list[dict[str, str]]:
    out = folder / f"{grain}.csv"
    line = [mf, "query", "--metrics", ",".join(metrics), "--decimals", str(DECIMALS),
            "--csv", str(out)]
    if grain == "month":
        line += ["--group-by", "metric_time__month", "--order", "metric_time__month"]
    elif grain == "run":
        line += ["--group-by", f"{RUN},metric_time__day", "--order", "metric_time__day"]
    call(*line)
    with out.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def day_of(text: str) -> date:
    return datetime.fromisoformat(text.strip()[:10]).date()


Value = tuple[str, str, date, str | None, float | None]   # metric, grain, period, run, value


def values(mf: str, metrics: list[dict[str, Any]]) -> list[Value]:
    totals = [m["metric"] for m in metrics if not m["offset"]]   # offsets need a time grain
    by_run_all = [m["metric"] for m in metrics if m["by_run"]]
    by_run = [m["metric"] for m in metrics if m["by_run"] and not m["offset"]]
    rows: list[Value] = []
    with tempfile.TemporaryDirectory() as folder:
        # Business and DQ metrics apart: queried together, each would get a
        # row for every month of the other (transaction months, run months).
        for group in ([m["metric"] for m in metrics if not m["by_run"]], by_run_all):
            if not group:
                continue
            for row in query(mf, group, Path(folder), "month"):
                month = day_of(row["metric_time__month"]).replace(day=1)
                rows += [(name, "month", month, None, number(row.get(name)))
                         for name in group]
        [total] = query(mf, totals, Path(folder), "total") or [{}]
        rows += [(name, "total", TOTAL_PERIOD, None, number(total.get(name)))
                 for name in totals]
        if by_run:
            for row in query(mf, by_run, Path(folder), "run"):
                run, day = row[RUN], day_of(row["metric_time__day"])
                rows += [(name, "run", day, run, number(row.get(name))) for name in by_run]
    return rows


# ============================================================
# Store
# ============================================================

def store(metrics: list[dict[str, Any]], rows: list[Value]) -> None:
    now = datetime.now(timezone.utc)
    columns = ("metric", "label", "description", "metric_type", "formula", "unit",
               "area", "headline")
    with bronze.connect() as conn:
        with conn.transaction():
            conn.execute(SQL_FILE.read_text(encoding="utf-8"))
            conn.execute("DELETE FROM semantic.metric_definition")   # values cascade
            for metric in metrics:
                conn.execute(
                    f"INSERT INTO semantic.metric_definition ({', '.join(columns)}, defined_at) "
                    f"VALUES ({', '.join(['%s'] * len(columns))}, %s)",
                    [*(metric[c] for c in columns), now],
                )
            with conn.cursor().copy(
                "COPY semantic.metric_value (metric, grain, period, run_id, value, computed_at) "
                "FROM STDIN"
            ) as copy:
                for row in rows:
                    copy.write_row((*row, now))


def main() -> int:
    global project
    dbt, mf = executables()
    with dbt_project.workspace() as project:
        call(dbt, "build", "--profiles-dir", str(project), "--exclude", "tag:dq")
        metrics = definitions()
        rows = values(mf, metrics)
    store(metrics, rows)

    months = len({period for _, grain, period, _, _ in rows if grain == "month"})
    runs = len({run for _, grain, _, run, _ in rows if grain == "run"})
    headline = [m for m in metrics if m["headline"]]
    print(f"Stored {len(metrics)} metrics ({len(headline)} headline) for {months} months "
          f"and {runs} runs in semantic.metric_definition / metric_value.")
    totals = {name: value for name, grain, _, _, value in rows if grain == "total"}
    area = None
    for metric in sorted(headline, key=lambda m: (m["area"] != DQ_AREA, m["area"] or "")):
        if metric["area"] != area:
            area = metric["area"]
            print(f"  {area}")
        value = totals.get(metric["metric"])
        shown = ("-" if value is None else f"{value:.2%}" if metric["unit"] == "rate"
                 else f"{value:,.0f}" if metric["unit"] == "count" else f"{value:,.2f}")
        print(f"    {metric['label']:<34} {shown:>20}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SemanticError as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)
