"""Build the Transight Banking Power BI project (PBIP) from the repository.

Writes, next to this tools/ folder:

  Transight Banking.pbip              open this in Power BI Desktop
  Transight Banking.SemanticModel/    TMDL model over PostgreSQL gold + semantic
  Transight Banking.Report/           PBIR report: 3 pages, drill-through, tooltip

The fact table measures are read from semantic/fact_transaction_measures.dax
(the project's single definition of them) and copied unchanged into the
model; only the measures the report needs on top of them (latest-year YoY,
data-quality ratios as defined in dbt/models/semantic/_metrics.yml) are
defined here.

    python powerbi/tools/build_pbip.py           # refuses to overwrite
    python powerbi/tools/build_pbip.py --force   # regenerate (loses edits
                                                 # made in Power BI Desktop)
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parents[1]
NAME = "Transight Banking"
DAX_FILE = ROOT / "semantic" / "fact_transaction_measures.dax"

SCHEMA = "https://developer.microsoft.com/json-schemas/fabric"
VC_SCHEMA = f"{SCHEMA}/item/report/definition/visualContainer/2.4.0/schema.json"
PAGE_SCHEMA = f"{SCHEMA}/item/report/definition/page/2.0.0/schema.json"
NS = uuid.UUID("6d1f3c52-6f0e-4a8e-9a43-1d0c2b7e5f10")

NAVY, TEAL, INK, MUTED, LINE = "#0B2545", "#13A89E", "#1B2B3A", "#5C6B7A", "#DCE3EA"
CANVAS = "#F4F7FA"


def tag(*parts: str) -> str:
    """Stable lineage tag / object id, so regenerating keeps the same ids."""
    return str(uuid.uuid5(NS, "|".join(parts)))


def vid(*parts: str) -> str:
    return tag(*parts).replace("-", "")[:20]


# =====================================================================
# Semantic model (TMDL)
# =====================================================================

FMT_COUNT = "#,0"
FMT_EGP = r"\E\G\P #,0"
FMT_EGP2 = r"\E\G\P #,0.00"
FMT_RATE = "0.0%"
FMT_RATE2 = "0.00%"
FMT_RATE3 = "0.000%"


def fact_format(name: str) -> str:
    """Formats from semantic/README.md (EGP shown with its currency code)."""
    per_txn = ("Avg ", "per ", "Per ", "Savings per", "Cost per")
    if name in ("Latest Transaction Date",):
        return "yyyy-mm-dd"
    if name == "Latest Load Run":
        return ""
    if name == "Assisted vs Digital Cost Multiple":
        return r"0.0\x"
    if name == "Avg Attempts per Transaction":
        return "0.00"
    if name in ("Avg Digital Session (min)", "Transactions per Customer"):
        return "0.0"
    if name == "Transactions per Branch":
        return FMT_COUNT
    if name == "Cost per 1K EGP Processed":
        return FMT_EGP2
    if "%" in name or "(pp)" in name:
        return FMT_RATE
    if "(EGP)" in name:
        return FMT_EGP2 if any(p in name for p in per_txn) else FMT_EGP
    return FMT_COUNT


def parse_dax_measures(path: Path) -> list[dict]:
    """MEASURE blocks of the project's DAX file, with their folder (from the
    section header) and description (from the comment above them)."""
    lines = path.read_text(encoding="utf-8").splitlines()
    measures, folder, pending, in_header, current = [], "", [], False, None
    start = next(i for i, l in enumerate(lines) if l.strip() == "DEFINE")
    for line in lines[start + 1:]:
        s = line.strip()
        if s.startswith("EVALUATE") or s.startswith("// Headline numbers"):
            break
        m = re.match(r"MEASURE '([^']+)'\[(.+)\] =\s*$", s)
        if m:
            current = {"table": m.group(1), "name": m.group(2), "folder": folder,
                       "description": " ".join(pending), "lines": []}
            measures.append(current)
            pending = []
            continue
        if s.startswith("// ----"):
            in_header = not in_header
            current = None
            continue
        if s.startswith("//"):
            current = None
            f = re.search(r"folder:\s*(.+)$", s)
            if f:
                folder = f.group(1).strip()
            elif not in_header:
                pending.append(s.lstrip("/ ").strip())
            continue
        if not s:
            current = None
            continue
        if current is not None:
            current["lines"].append(line.rstrip())
    for m in measures:
        indent = min(len(l) - len(l.lstrip()) for l in m["lines"])
        m["lines"] = [l[indent:] for l in m["lines"]]
    return measures


# Measures this report adds. Each is (table, name, folder, format, description, DAX).
NEW_MEASURES = [
    # --- latest-year comparisons for KPI cards (YoY of the whole range is
    #     meaningless: SAMEPERIODLASTYEAR of 2022-2025 is 2022-2024)
    ("fact_transaction", "Latest Year in Selection", "11 Report", "0",
     "Calendar year of the latest transaction in the current selection.",
     ["YEAR ( [Latest Transaction Date] )"]),
    ("fact_transaction", "Transactions YoY % (Latest Year)", "11 Report", "+0.0%;-0.0%;0.0%",
     "Transactions YoY % of the latest year in the selection against the year before.",
     ["VAR LatestYear = [Latest Year in Selection]",
      "RETURN",
      "    CALCULATE ( [Transactions YoY %], 'dim_date'[year] = LatestYear )"]),
    ("fact_transaction", "Transaction Value YoY % (Latest Year)", "11 Report", "+0.0%;-0.0%;0.0%",
     "Completed value YoY % of the latest year in the selection against the year before.",
     ["VAR LatestYear = [Latest Year in Selection]",
      "RETURN",
      "    CALCULATE ( [Transaction Value YoY %], 'dim_date'[year] = LatestYear )"]),
    ("fact_transaction", "Title Volume YoY", "11 Report", "",
     "Card title: which years the volume growth compares.",
     ["VAR LatestYear = [Latest Year in Selection]",
      "RETURN",
      "    IF ( NOT ISBLANK ( LatestYear ),",
      "        \"Volume YoY \" & LatestYear & \" vs \" & LatestYear - 1,",
      "        \"Volume YoY\" )"]),
    ("fact_transaction", "Title Value YoY", "11 Report", "",
     "Card title: which years the value growth compares.",
     ["VAR LatestYear = [Latest Year in Selection]",
      "RETURN",
      "    IF ( NOT ISBLANK ( LatestYear ),",
      "        \"Value YoY \" & LatestYear & \" vs \" & LatestYear - 1,",
      "        \"Value YoY\" )"]),
    # (FirstDate / LastDate are DAX functions: not usable as variable names)
    ("fact_transaction", "Data Period", "11 Report", "",
     "Header text: first and last transaction date in the selection.",
     ["VAR StartDate = MIN ( 'fact_transaction'[transaction_date] )",
      "VAR EndDate = [Latest Transaction Date]",
      "RETURN",
      "    IF ( NOT ISBLANK ( EndDate ),",
      "        \"Data: \" & FORMAT ( StartDate, \"d mmm yyyy\" ) & \" – \" & FORMAT ( EndDate, \"d mmm yyyy\" ) )"]),
    ("fact_transaction", "Selected Branch", "11 Report", "",
     "Drill-through header: the branch shown.",
     ["SELECTEDVALUE ( 'dim_branch'[branch_name], \"All branches\" )"]),
    ("fact_transaction", "Selected Branch Location", "11 Report", "",
     "Drill-through header: city, governorate and region of the branch.",
     ["IF (",
      "    HASONEVALUE ( 'dim_branch'[branch_id] ),",
      "    SELECTEDVALUE ( 'dim_branch'[branch_city] ) & \", \" & SELECTEDVALUE ( 'dim_branch'[branch_governorate] )",
      "        & \" · \" & SELECTEDVALUE ( 'dim_branch'[branch_region] ),",
      "    \"Open this page by drill-through from a branch\"",
      ")"]),
    # --- data quality: same definitions as dbt/models/semantic/_metrics.yml
    ("dq_runs", "Validated Runs", "DQ Records", FMT_COUNT, "validated_runs: count(run_id)",
     ["DISTINCTCOUNT ( 'dq_runs'[run_id] )"]),
    ("dq_runs", "Processed Records", "DQ Records", FMT_COUNT, "processed_records: sum(processed)",
     ["SUM ( 'dq_runs'[processed] )"]),
    ("dq_runs", "Valid Records", "DQ Records", FMT_COUNT, "valid_records: sum(valid)",
     ["SUM ( 'dq_runs'[valid] )"]),
    ("dq_runs", "Clean Records", "DQ Records", FMT_COUNT, "clean_records: sum(clean_records) = valid + resolved",
     ["SUM ( 'dq_runs'[clean_records] )"]),
    ("dq_runs", "Records with Issues", "DQ Records", FMT_COUNT, "records_with_issues: sum(records_with_issues)",
     ["SUM ( 'dq_runs'[records_with_issues] )"]),
    ("dq_runs", "Auto-Fixed Records", "DQ Records", FMT_COUNT, "auto_fixed_records: sum(auto_fixed_records)",
     ["SUM ( 'dq_runs'[auto_fixed_records] )"]),
    ("dq_runs", "Quarantined Records", "DQ Records", FMT_COUNT, "quarantined_records: sum(quarantined)",
     ["SUM ( 'dq_runs'[quarantined] )"]),
    ("dq_runs", "Resolved Records", "DQ Records", FMT_COUNT, "resolved_records: sum(resolved)",
     ["SUM ( 'dq_runs'[resolved] )"]),
    ("dq_runs", "Unresolved Records", "DQ Records", FMT_COUNT, "unresolved_records: sum(unresolved_records)",
     ["SUM ( 'dq_runs'[unresolved_records] )"]),
    ("dq_runs", "Manually Fixed Records", "DQ Records", FMT_COUNT, "manually_fixed_records: sum(manually_fixed_records)",
     ["SUM ( 'dq_runs'[manually_fixed_records] )"]),
    ("dq_runs", "Discarded Records", "DQ Records", FMT_COUNT, "discarded_records: sum(discarded)",
     ["SUM ( 'dq_runs'[discarded] )"]),
    ("dq_runs", "Initial DQ Score %", "DQ Scores", FMT_RATE2, "initial_dq_score: valid_records / processed_records",
     ["DIVIDE ( [Valid Records], [Processed Records] )"]),
    ("dq_runs", "Final DQ Score %", "DQ Scores", FMT_RATE2, "final_dq_score: clean_records / processed_records",
     ["DIVIDE ( [Clean Records], [Processed Records] )"]),
    ("dq_runs", "DQ Score Uplift (pp)", "DQ Scores", "+0.0000%;-0.0000%;0.0000%",
     "dq_score_uplift: final_dq_score - initial_dq_score (percentage points)",
     ["[Final DQ Score %] - [Initial DQ Score %]"]),
    ("dq_runs", "Auto-Fix Rate %", "DQ Scores", FMT_RATE, "auto_fix_rate: auto_fixed_records / records_with_issues",
     ["DIVIDE ( [Auto-Fixed Records], [Records with Issues] )"]),
    ("dq_runs", "Quarantine Rate %", "DQ Scores", FMT_RATE2, "quarantine_rate: quarantined_records / processed_records",
     ["DIVIDE ( [Quarantined Records], [Processed Records] )"]),
    ("dq_runs", "Quarantine Resolution Rate %", "DQ Scores", FMT_RATE3, "resolution_rate: resolved_records / quarantined_records",
     ["DIVIDE ( [Resolved Records], [Quarantined Records] )"]),
    ("dq_rule_checks", "Values Checked", "DQ Rules", FMT_COUNT, "values_checked: sum(checked), first validation round",
     ["SUM ( 'dq_rule_checks'[checked] )"]),
    ("dq_rule_checks", "Values Failed", "DQ Rules", FMT_COUNT, "values_failed: sum(failed)",
     ["SUM ( 'dq_rule_checks'[failed] )"]),
    ("dq_rule_checks", "Values Repaired", "DQ Rules", FMT_COUNT, "values_repaired: sum(repaired)",
     ["SUM ( 'dq_rule_checks'[repaired] )"]),
    ("dq_rule_checks", "Values Unresolved", "DQ Rules", FMT_COUNT, "sum(unresolved)",
     ["SUM ( 'dq_rule_checks'[unresolved] )"]),
    ("dq_rule_checks", "Rule Failure Rate %", "DQ Rules", FMT_RATE3, "rule_failure_rate: values_failed / values_checked",
     ["DIVIDE ( [Values Failed], [Values Checked] )"]),
    ("dq_rule_checks", "Rule Repair Rate %", "DQ Rules", FMT_RATE, "rule_repair_rate: values_repaired / values_failed",
     ["DIVIDE ( [Values Repaired], [Values Failed] )"]),
    ("dq_rule_checks", "Rules Evaluated", "DQ Rules", FMT_COUNT, "distinct rules checked",
     ["DISTINCTCOUNT ( 'dq_rule_checks'[rule_id] )"]),
    ("dq_rule_checks", "Rules with Failures", "DQ Rules", FMT_COUNT, "distinct rules that failed at least one value",
     ["CALCULATE ( DISTINCTCOUNT ( 'dq_rule_checks'[rule_id] ), 'dq_rule_checks'[failed] > 0 )"]),
    ("dq_correction_summary", "Corrections", "DQ Corrections", FMT_COUNT, "rows of semantic.dq_correction",
     ["SUM ( 'dq_correction_summary'[correction_count] )"]),
    ("dq_quarantine_status", "Quarantined Records (Latest Status)", "DQ Corrections", FMT_COUNT,
     "quarantined records by their latest status (semantic.dq_quarantine is a status log)",
     ["SUM ( 'dq_quarantine_status'[records] )"]),
]


def m_source(body: list[str]) -> list[str]:
    return ["let", "    Source = PostgreSQL.Database(PgServer, PgDatabase),"] + body


FACT_COLUMNS = [
    # name, dataType, format, hidden
    ("transaction_date", "dateTime", "yyyy-mm-dd", True),
    ("transaction_type", "string", None, False),
    ("transaction_status", "string", None, False),
    ("channel", "string", None, False),
    ("channel_group", "string", None, False),
    ("channel_feature", "string", None, False),
    ("digital_feature_use", "string", None, False),
    ("currency", "string", None, False),
    ("transaction_amount", "double", "#,0.00", True),
    ("amount_egp", "double", r"\E\G\P #,0.00", True),
    ("transaction_fee", "double", "#,0.00", True),
    ("estimated_channel_cost_egp", "double", r"\E\G\P #,0.00", True),
    ("card_type", "string", None, False),
    ("merchant_name", "string", None, False),
    ("merchant_category", "string", None, False),
    ("login_success", "boolean", None, False),
    ("session_duration_seconds", "double", "#,0", True),
    ("attempt_count", "int64", "0", False),
    ("failure_reason", "string", None, False),
    ("digital_channel_flag", "boolean", None, False),
    ("digital_migration_opportunity", "boolean", None, False),
    ("account_id", "string", None, True),
    ("bank_id", "string", None, True),
    ("branch_id", "string", None, True),
    ("customer_id", "string", None, True),
    ("run_id", "string", None, False),
]

FACT_M = m_source([
    '    Fact = Source{[Schema = "gold", Item = "fact_transaction"]}[Data],',
    "    Selected = Table.SelectColumns(Fact, {"
    + ", ".join(f'"{c[0]}"' for c in FACT_COLUMNS if c[0] != "channel_group") + "}),",
    "    // money stays a decimal number (not fixed decimal): fixed decimal rounds",
    "    // every intermediate result, e.g. the digital cost per transaction, to 4",
    "    // places, and Potential Cost Savings would then miss measure_checks.sql",
    "    // the same split as digital_channel_flag: Digital = Mobile App, Internet",
    "    // Banking, InstaPay; every other channel (ATM, Branch, POS, Call Center,",
    "    // System, Other) is Assisted",
    '    WithGroup = Table.AddColumn(Selected, "channel_group",',
    '        each if [digital_channel_flag] then "Digital" else "Assisted", type text)',
    "in",
    "    WithGroup",
])

DIM_CUSTOMER_M = m_source([
    '    Customers = Source{[Schema = "gold", Item = "dim_customer"]}[Data],',
    "    // no names, e-mail or phone: the report does not need personal data",
    "    Selected = Table.Buffer(Table.SelectColumns(Customers, {",
    '        "customer_id", "gender", "date_of_birth", "marital_status", "customer_city",',
    '        "customer_governorate", "customer_segment", "occupation", "annual_income",',
    '        "credit_score", "customer_since_date"})),',
    "    // age on the last day the data covers (gold.dim_date spans the facts)",
    '    RefDate = List.Max(Source{[Schema = "gold", Item = "dim_date"]}[Data][date_key]),',
    '    WithAge = Table.AddColumn(Selected, "age_at_data_end", each',
    "        Date.Year(RefDate) - Date.Year([date_of_birth])",
    "            - (if Date.Month([date_of_birth]) * 100 + Date.Day([date_of_birth])",
    "                > Date.Month(RefDate) * 100 + Date.Day(RefDate) then 1 else 0), Int64.Type),",
    '    WithAgeOrder = Table.AddColumn(WithAge, "age_band_order", each',
    "        let a = [age_at_data_end] in",
    "        if a < 25 then 1 else if a < 35 then 2 else if a < 45 then 3",
    "        else if a < 55 then 4 else if a < 65 then 5 else 6, Int64.Type),",
    '    WithAgeBand = Table.AddColumn(WithAgeOrder, "age_band", each',
    '        {"18-24", "25-34", "35-44", "45-54", "55-64", "65+"}{[age_band_order] - 1}, type text),',
    "    // income quintiles: five equal-sized customer groups by annual_income",
    '    Cuts = List.Percentile(List.Buffer(Selected[annual_income]), {0.2, 0.4, 0.6, 0.8}),',
    '    WithQuintile = Table.AddColumn(WithAgeBand, "income_quintile", each',
    "        let x = [annual_income] in",
    "        if x <= Cuts{0} then 1 else if x <= Cuts{1} then 2 else if x <= Cuts{2} then 3",
    "        else if x <= Cuts{3} then 4 else 5, Int64.Type),",
    '    Short = (n as number) as text => if n >= 1000000 then Number.ToText(n / 1000000, "0.0", "en-US") & "M"',
    '        else Number.ToText(n / 1000, "0", "en-US") & "K",',
    '    Ranges = Table.Group(WithQuintile, {"income_quintile"}, {',
    '        {"lo", each List.Min([annual_income]), type number},',
    '        {"hi", each List.Max([annual_income]), type number}}),',
    '    Labels = Table.AddColumn(Ranges, "income_band", each',
    '        "Q" & Text.From([income_quintile]) & ": " & Short([lo]) & "–" & Short([hi]) & " EGP", type text),',
    '    Joined = Table.Join(WithQuintile, "income_quintile",',
    '        Table.SelectColumns(Labels, {"income_quintile", "income_band"}), "income_quintile"),',
    '    Result = Table.RemoveColumns(Joined, {"date_of_birth"})',
    "in",
    "    Result",
])


def nav_m(schema: str, item: str, columns: list[str] | None = None) -> list[str]:
    body = [f'    Data = Source{{[Schema = "{schema}", Item = "{item}"]}}[Data]' + ("," if columns else "")]
    if columns:
        body.append("    Selected = Table.SelectColumns(Data, {" + ", ".join(f'"{c}"' for c in columns) + "})")
        return m_source(body) + ["in", "    Selected"]
    return m_source(body) + ["in", "    Data"]


DQ_CORRECTION_M = m_source([
    '    Corrections = Source{[Schema = "semantic", Item = "dq_correction"]}[Data],',
    "    // one row per kind of correction (the detail holds every changed value)",
    "    Grouped = Table.Group(Corrections,",
    '        {"run_id", "dataset", "round", "correction_type", "action", "outcome", "rule_id", "column"},',
    '        {{"correction_count", each Table.RowCount(_), Int64.Type}}),',
    '    Renamed = Table.RenameColumns(Grouped, {{"column", "column_name"}})',
    "in",
    "    Renamed",
])

DQ_QUARANTINE_M = m_source([
    '    Log = Source{[Schema = "semantic", Item = "dq_quarantine"]}[Data],',
    "    // dq_quarantine logs every status change: keep each record's latest one",
    '    Selected = Table.SelectColumns(Log, {"run_id", "dataset", "record_id", "sequence", "status"}),',
    '    Latest = Table.Group(Selected, {"run_id", "dataset", "record_id"},',
    '        {{"status", each Table.Max(_, "sequence")[status], type text}}),',
    '    Counted = Table.Group(Latest, {"run_id", "dataset", "status"},',
    '        {{"records", each Table.RowCount(_), Int64.Type}})',
    "in",
    "    Counted",
])

# table name -> (description, M, [(column, dataType, format, hidden, extra props)])
TABLES: dict[str, dict] = {
    "fact_transaction": {
        "description": "gold.fact_transaction: one row per transaction (latest run per key).",
        "m": FACT_M,
        "columns": [(c, t, f, h, {}) for c, t, f, h in FACT_COLUMNS],
    },
    "dim_date": {
        "description": "gold.dim_date: one row per day the facts cover. Marked as the date table.",
        "m": nav_m("gold", "dim_date"),
        "date_table": True,
        "columns": [
            ("date_key", "dateTime", "yyyy-mm-dd", False, {"isKey": True}),
            ("year", "int64", "0", False, {}),
            ("quarter", "int64", "0", False, {}),
            ("month", "int64", "0", False, {}),
            ("month_name", "string", None, False, {"sortByColumn": "month"}),
            ("year_month", "string", None, False, {}),
            ("day", "int64", "0", False, {}),
            ("day_of_week", "int64", "0", False, {}),
            ("day_name", "string", None, False, {"sortByColumn": "day_of_week"}),
            ("week_of_year", "int64", "0", False, {}),
            ("is_weekend", "boolean", None, False, {}),
        ],
    },
    "dim_customer": {
        "description": "gold.dim_customer without personal data, plus age band (age on the last data day) and income quintile.",
        "m": DIM_CUSTOMER_M,
        "columns": [
            ("customer_id", "string", None, False, {}),
            ("gender", "string", None, False, {}),
            ("marital_status", "string", None, False, {}),
            ("customer_city", "string", None, False, {}),
            ("customer_governorate", "string", None, False, {}),
            ("customer_segment", "string", None, False, {}),
            ("occupation", "string", None, False, {}),
            ("annual_income", "double", "#,0", False, {}),
            ("credit_score", "int64", "0", False, {}),
            ("customer_since_date", "dateTime", "yyyy-mm-dd", False, {}),
            ("age_at_data_end", "int64", "0", False, {}),
            ("age_band_order", "int64", "0", True, {}),
            ("age_band", "string", None, False, {"sortByColumn": "age_band_order"}),
            ("income_quintile", "int64", "0", True, {}),
            ("income_band", "string", None, False, {"sortByColumn": "income_quintile"}),
        ],
    },
    "dim_account": {
        "description": "gold.dim_account.",
        "m": nav_m("gold", "dim_account", ["account_id", "account_type", "account_open_date", "account_status", "interest_rate"]),
        "columns": [
            ("account_id", "string", None, False, {}),
            ("account_type", "string", None, False, {}),
            ("account_open_date", "dateTime", "yyyy-mm-dd", False, {}),
            ("account_status", "string", None, False, {}),
            ("interest_rate", "double", "0.00", False, {}),
        ],
    },
    "dim_branch": {
        "description": "gold.dim_branch (not related to dim_bank: the fact carries bank_id).",
        "m": nav_m("gold", "dim_branch", ["branch_id", "branch_name", "branch_city", "branch_governorate", "branch_region"]),
        "columns": [
            ("branch_id", "string", None, False, {}),
            ("branch_name", "string", None, False, {}),
            ("branch_city", "string", None, False, {}),
            ("branch_governorate", "string", None, False, {}),
            ("branch_region", "string", None, False, {}),
        ],
    },
    "dim_bank": {
        "description": "gold.dim_bank.",
        "m": nav_m("gold", "dim_bank"),
        "columns": [
            ("bank_id", "string", None, False, {}),
            ("bank_name", "string", None, False, {}),
            ("bank_type", "string", None, False, {}),
        ],
    },
    "dq_runs": {
        "description": "semantic.dq_runs: one row per validated run, figures of its latest correction round.",
        "m": nav_m("semantic", "dq_runs"),
        "columns": [
            ("run_id", "string", None, False, {}),
            ("dataset", "string", None, False, {}),
            ("source_file", "string", None, False, {}),
            ("run_date", "dateTime", "yyyy-mm-dd", False, {}),
            ("schema_version", "string", None, False, {}),
            ("rules_version", "string", None, False, {}),
        ] + [(c, "int64", "#,0", True, {}) for c in (
            "rounds", "processed", "valid", "valid_with_warnings", "auto_fixed_records", "quarantined",
            "records_with_issues", "resolved", "clean_records", "manually_fixed_records",
            "confirmed_records", "discarded", "unresolved_records")],
    },
    "dq_run_rounds": {
        "description": "semantic.dq_run: one row per run and correction round (round history).",
        "m": nav_m("semantic", "dq_run", [
            "run_id", "dataset", "round", "round_type", "started_at", "finished_at", "processed",
            "records_with_issues", "auto_fixed_records", "quarantined", "round_revalidated",
            "round_resolved", "round_failed_again", "resolved", "unresolved_records",
            "initial_dq_score", "final_dq_score", "is_latest_round"]),
        "columns": [
            ("run_id", "string", None, False, {}),
            ("dataset", "string", None, False, {}),
            ("round", "int64", "0", False, {}),
            ("round_type", "string", None, False, {}),
            ("started_at", "dateTime", "yyyy-mm-dd hh:nn", False, {}),
            ("finished_at", "dateTime", "yyyy-mm-dd hh:nn", False, {}),
        ] + [(c, "int64", "#,0", False, {}) for c in (
            "processed", "records_with_issues", "auto_fixed_records", "quarantined", "round_revalidated",
            "round_resolved", "round_failed_again", "resolved", "unresolved_records")] + [
            ("initial_dq_score", "double", "0.0000%", False, {}),
            ("final_dq_score", "double", "0.0000%", False, {}),
            ("is_latest_round", "boolean", None, False, {}),
        ],
    },
    "dq_rule_checks": {
        "description": "semantic.dq_rule_checks: one row per run and rule of the first validation round.",
        "m": nav_m("semantic", "dq_rule_checks"),
        "columns": [
            ("rule_check_id", "string", None, True, {}),
            ("run_id", "string", None, False, {}),
            ("dataset", "string", None, False, {}),
            ("run_date", "dateTime", "yyyy-mm-dd", False, {}),
            ("rule_id", "string", None, False, {}),
            ("rule_name", "string", None, False, {}),
            ("rule_type", "string", None, False, {}),
            ("column_name", "string", None, False, {}),
            ("dq_dimension", "string", None, False, {}),
            ("severity", "string", None, False, {}),
            ("error_code", "string", None, False, {}),
            ("origin", "string", None, False, {}),
        ] + [(c, "int64", "#,0", True, {}) for c in ("checked", "failed", "repaired", "unresolved")],
    },
    "dq_correction_summary": {
        "description": "semantic.dq_correction grouped by run, round, type, action, outcome, rule and column.",
        "m": DQ_CORRECTION_M,
        "columns": [
            ("run_id", "string", None, False, {}),
            ("dataset", "string", None, False, {}),
            ("round", "int64", "0", False, {}),
            ("correction_type", "string", None, False, {}),
            ("action", "string", None, False, {}),
            ("outcome", "string", None, False, {}),
            ("rule_id", "string", None, False, {}),
            ("column_name", "string", None, False, {}),
            ("correction_count", "int64", "#,0", True, {}),
        ],
    },
    "dq_quarantine_status": {
        "description": "semantic.dq_quarantine: records per latest status.",
        "m": DQ_QUARANTINE_M,
        "columns": [
            ("run_id", "string", None, False, {}),
            ("dataset", "string", None, False, {}),
            ("status", "string", None, False, {}),
            ("records", "int64", "#,0", True, {}),
        ],
    },
}

RELATIONSHIPS = [
    ("fact_transaction", "transaction_date", "dim_date", "date_key"),
    ("fact_transaction", "customer_id", "dim_customer", "customer_id"),
    ("fact_transaction", "account_id", "dim_account", "account_id"),
    ("fact_transaction", "branch_id", "dim_branch", "branch_id"),
    ("fact_transaction", "bank_id", "dim_bank", "bank_id"),
    ("dq_rule_checks", "run_id", "dq_runs", "run_id"),
    ("dq_run_rounds", "run_id", "dq_runs", "run_id"),
    ("dq_correction_summary", "run_id", "dq_runs", "run_id"),
    ("dq_quarantine_status", "run_id", "dq_runs", "run_id"),
]


def q(name: str) -> str:
    """TMDL object name, quoted when needed."""
    return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) else "'" + name.replace("'", "''") + "'"


def tmdl_measure(table: str, name: str, lines: list[str], fmt: str, folder: str, desc: str) -> list[str]:
    out = []
    if desc:
        out.append(f"\t/// {desc}")
    if len(lines) == 1:
        out.append(f"\tmeasure {q(name)} = {lines[0]}")
    else:
        out.append(f"\tmeasure {q(name)} =")
        out += [f"\t\t\t{l}" for l in lines]
    if fmt:
        out.append(f"\t\tformatString: {fmt}")
    out.append(f"\t\tdisplayFolder: {folder}")
    out.append(f"\t\tlineageTag: {tag('measure', table, name)}")
    out.append("")
    return out


def tmdl_table(name: str, spec: dict, measures: list[dict]) -> str:
    out = [f"/// {spec['description']}", f"table {q(name)}", f"\tlineageTag: {tag('table', name)}"]
    if spec.get("date_table"):
        out.append("\tdataCategory: Time")
    out.append("")
    for m in measures:
        out += tmdl_measure(name, m["name"], m["lines"], m["format"], m["folder"], m["description"])
    for col, dtype, fmt, hidden, extra in spec["columns"]:
        out.append(f"\tcolumn {q(col)}")
        out.append(f"\t\tdataType: {dtype}")
        if extra.get("isKey"):
            out.append("\t\tisKey")
        if hidden:
            out.append("\t\tisHidden")
        if fmt:
            out.append(f"\t\tformatString: {fmt}")
        out.append(f"\t\tlineageTag: {tag('column', name, col)}")
        out.append("\t\tsummarizeBy: none")
        out.append(f"\t\tsourceColumn: {col}")
        if "sortByColumn" in extra:
            out.append(f"\t\tsortByColumn: {extra['sortByColumn']}")
        out.append("")
        if dtype == "dateTime":
            out.append("\t\tannotation UnderlyingDateTimeDataType = Date")
            out.append("")
    out.append(f"\tpartition {q(name)} = m")
    out.append("\t\tmode: import")
    out.append("\t\tsource =")
    out += [f"\t\t\t\t{l}" for l in spec["m"]]
    out.append("")
    return "\n".join(out) + "\n"


def build_model(base: Path) -> dict[str, list[str]]:
    sm = base / f"{NAME}.SemanticModel"
    d = sm / "definition"
    (d / "tables").mkdir(parents=True)
    write_json(sm / "definition.pbism", {
        "$schema": f"{SCHEMA}/item/semanticModel/definitionProperties/1.0.0/schema.json",
        "version": "4.2",
        "settings": {},
    })
    (d / "database.tmdl").write_text("database\n\tcompatibilityLevel: 1600\n\n", encoding="utf-8")

    measures_by_table: dict[str, list[dict]] = {t: [] for t in TABLES}
    for m in parse_dax_measures(DAX_FILE):
        m["format"] = fact_format(m["name"])
        # semantic/fact_transaction_measures.dax names a variable PreviousMonth,
        # which is a DAX function: the measure does not compile. Renamed here
        # only; the source file is left as it is.
        m["lines"] = [re.sub(r"\bPreviousMonth\b", "PriorMonth", l) for l in m["lines"]]
        measures_by_table[m["table"]].append(m)
    for table, name, folder, fmt, desc, lines in NEW_MEASURES:
        measures_by_table[table].append(
            {"name": name, "folder": folder, "format": fmt, "description": desc, "lines": lines})

    # Power BI names are case-insensitive: a measure may share its name with
    # no other measure and no column.
    names = {c[0].lower(): f"column {t}[{c[0]}]" for t, spec in TABLES.items() for c in spec["columns"]}
    for t, ms in measures_by_table.items():
        for m in ms:
            clash = names.get(m["name"].lower())
            if clash:
                raise SystemExit(f"measure {t}[{m['name']}] clashes with {clash}")
            names[m["name"].lower()] = f"measure {t}[{m['name']}]"

    order = ["PgServer", "PgDatabase"] + list(TABLES)
    model = [
        "model Model",
        "\tculture: en-US",
        "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
        "\tsourceQueryCulture: en-US",
        "\tdataAccessOptions",
        "\t\tlegacyRedirects",
        "\t\treturnErrorValuesAsNull",
        "",
        "annotation __PBI_TimeIntelligenceEnabled = 0",
        "",
        "annotation PBI_QueryOrder = " + json.dumps(order, separators=(",", ":")),
        "",
    ] + [f"ref table {q(t)}" for t in TABLES]
    (d / "model.tmdl").write_text("\n".join(model) + "\n\n", encoding="utf-8")

    expressions = []
    for pname, value, desc in (
        ("PgServer", "localhost:5432", "PostgreSQL host:port (POSTGRES_HOST:POSTGRES_PORT in .env)"),
        ("PgDatabase", "metaflow", "PostgreSQL database (POSTGRES_DB in .env)"),
    ):
        expressions += [
            f"/// {desc}",
            f'expression {pname} = "{value}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
            f"\tlineageTag: {tag('expression', pname)}",
            "",
            "\tannotation PBI_ResultType = Text",
            "",
        ]
    (d / "expressions.tmdl").write_text("\n".join(expressions) + "\n", encoding="utf-8")

    rels = []
    for ft, fc, tt, tc in RELATIONSHIPS:
        rels += [f"relationship {tag('rel', ft, fc, tt, tc)}",
                 f"\tfromColumn: {q(ft)}.{q(fc)}",
                 f"\ttoColumn: {q(tt)}.{q(tc)}", ""]
    (d / "relationships.tmdl").write_text("\n".join(rels) + "\n", encoding="utf-8")

    for t, spec in TABLES.items():
        (d / "tables" / f"{t}.tmdl").write_text(tmdl_table(t, spec, measures_by_table[t]), encoding="utf-8")
    return {t: [m["name"] for m in ms] for t, ms in measures_by_table.items()}


# =====================================================================
# Report (PBIR)
# =====================================================================

def lit(v) -> dict:
    if isinstance(v, bool):
        s = "true" if v else "false"
    elif isinstance(v, int):
        s = f"{v}L"
    elif isinstance(v, float):
        s = f"{v}D"
    else:
        s = "'" + str(v).replace("'", "''") + "'"
    return {"expr": {"Literal": {"Value": s}}}


def color(hex_: str) -> dict:
    return {"solid": {"color": lit(hex_)}}


def C(entity: str, prop: str) -> dict:
    return {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}}


def M(prop: str, entity: str = "fact_transaction") -> dict:
    return {"Measure": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}}


def proj(field: dict, display: str | None = None) -> dict:
    kind = "Column" if "Column" in field else "Measure"
    ent = field[kind]["Expression"]["SourceRef"]["Entity"]
    prop = field[kind]["Property"]
    p = {"field": field, "queryRef": f"{ent}.{prop}", "nativeQueryRef": prop}
    if display:
        p["displayName"] = display
    return p


def props(**kw) -> list[dict]:
    return [{"properties": kw}]


class Page:
    def __init__(self, name: str, display: str, width: int = 1280, height: int = 760, **extra):
        self.name, self.display, self.width, self.height, self.extra = name, display, width, height, extra
        self.visuals: list[dict] = []

    def add(self, key: str, x, y, w, h, visual: dict | None = None, **container) -> str:
        name = vid(self.name, key)
        vc = {"$schema": VC_SCHEMA, "name": name,
              "position": {"x": x, "y": y, "z": len(self.visuals) * 1000, "height": h, "width": w,
                           "tabOrder": len(self.visuals) * 1000}}
        if visual is not None:
            vc["visual"] = visual
        vc.update(container)
        self.visuals.append(vc)
        return name


def container_objects(title: str | dict | None = None, subtitle: str | None = None,
                      tooltip_page: str | None = None, background: str | None = None,
                      border: bool | None = None) -> dict:
    o: dict = {}
    if title is None:
        o["title"] = props(show=lit(False))
    else:
        # a measure as title text: the card names the years it compares
        o["title"] = props(show=lit(True), text={"expr": title} if isinstance(title, dict) else lit(title))
    if subtitle:
        o["subTitle"] = props(show=lit(True), text=lit(subtitle))
    if tooltip_page:
        o["visualTooltip"] = props(show=lit(True), type=lit("ReportPage"), section=lit(tooltip_page))
    if background:
        o["background"] = props(show=lit(True), color=color(background), transparency=lit(0.0))
    if border is not None:
        o["border"] = props(show=lit(border))
    return o


def visual(vtype: str, roles: dict[str, list[dict]] | None = None, sort: list[tuple[dict, str]] | None = None,
           objects: dict | None = None, vco: dict | None = None, **extra) -> dict:
    v: dict = {"visualType": vtype}
    if roles is not None:
        v["query"] = {"queryState": {r: {"projections": ps} for r, ps in roles.items()}}
        if sort:
            v["query"]["sortDefinition"] = {
                "sort": [{"field": f, "direction": d} for f, d in sort], "isDefaultSort": False}
    if objects:
        v["objects"] = objects
    if vco:
        v["visualContainerObjects"] = vco
    v["drillFilterOtherVisuals"] = True
    v.update(extra)
    return v


def card(measure: dict, title, display=None) -> dict:
    # counts in full (1,016,522), EGP totals in units with 2 decimals
    # (EGP 11.03bn), rates and per-transaction values as their format says
    name = measure["Measure"]["Property"]
    labels = {"fontSize": lit(16.0), "color": color(NAVY)}
    if "%" in name or "(pp)" in name or "Multiple" in name:
        pass
    elif "(EGP)" in name:
        labels["labelPrecision"] = lit(2)
    else:
        labels["labelDisplayUnits"] = lit(1.0)
    vco = container_objects(title)
    vco["title"][0]["properties"]["titleWrap"] = lit(True)
    return visual("card", {"Values": [proj(measure, display)]},
                  objects={"labels": [{"properties": labels}],
                           "categoryLabels": props(show=lit(False))},
                  vco=vco)


def slicer(field: dict, title: str, sync: str | None = None) -> dict:
    extra = {}
    if sync:
        extra["syncGroup"] = {"groupName": sync, "fieldChanges": True, "filterChanges": True}
    return visual("slicer", {"Values": [proj(field, title)]},
                  objects={"data": props(mode=lit("Dropdown")),
                           "header": props(show=lit(True), fontColor=color(NAVY))},
                  vco=container_objects(None), **extra)


def textbox(runs: list[tuple[str, dict]], bg: str | None = None) -> dict:
    paragraphs = [{"textRuns": [{"value": text, "textStyle": style}]} for text, style in runs]
    return {"visualType": "textbox",
            "objects": {"general": [{"properties": {"paragraphs": paragraphs}}]},
            "visualContainerObjects": {**container_objects(None, background=bg, border=False),
                                       "dropShadow": props(show=lit(False))},
            "drillFilterOtherVisuals": True}


def chart_objects(labels: bool = True, legend: bool | None = None) -> dict:
    o = {"labels": props(show=lit(labels))}
    if legend is not None:
        o["legend"] = props(show=lit(legend), position=lit("Top"))
    return o


def header(page: Page, title: str, subtitle: str, navigator: bool = True) -> None:
    page.add("header-bg", 0, 0, page.width, 64, textbox([
        (title, {"fontFamily": "Segoe UI Semibold", "fontSize": "20pt", "color": "#FFFFFF"}),
        (subtitle, {"fontFamily": "Segoe UI", "fontSize": "10pt", "color": "#BFD7EA"}),
    ], bg=NAVY))
    if navigator:
        page.add("nav", 700, 14, 564, 38, {
            "visualType": "pageNavigator",
            "objects": {
                "fill": [{"properties": {"show": lit(True), "fillColor": color("#13355C")}, "selector": {"id": "default"}},
                         {"properties": {"fillColor": color(TEAL)}, "selector": {"id": "selected"}}],
                "text": [{"properties": {"fontColor": color("#FFFFFF"), "fontSize": lit(10.0)}, "selector": {"id": "default"}}],
                "outline": [{"properties": {"show": lit(False)}, "selector": {"id": "default"}}],
                # the drill-through and tooltip pages are reached from visuals
                "pages": props(showHiddenPages=lit(False), showTooltipPages=lit(False)),
            },
            "visualContainerObjects": {**container_objects(None, border=False),
                                       "background": props(show=lit(False)),
                                       "dropShadow": props(show=lit(False))},
            "drillFilterOtherVisuals": True})


def kpi_row(page: Page, y: int, cards: list[tuple]) -> None:
    gap, left, total = 8, 16, page.width - 32
    w = (total - gap * (len(cards) - 1)) / len(cards)
    for i, c in enumerate(cards):
        measure, title = c[0], c[1]
        page.add(f"kpi{i}", round(left + i * (w + gap), 1), y, round(w, 1), 92, card(measure, title))


def footnote(page: Page, text: str) -> None:
    page.add("footnote", 16, page.height - 30, page.width - 32, 26, textbox(
        [(text, {"fontFamily": "Segoe UI", "fontSize": "8pt", "color": MUTED})]))


# ---- pages ----------------------------------------------------------

F = "fact_transaction"
TOOLTIP_PAGE = "ChannelTooltip"
DRILL_PAGE = "BranchDetail"


def page_executive() -> Page:
    p = Page("ExecutiveOverview", "Executive Banking Overview")
    header(p, "Executive Banking Overview",
           "Transaction volume, value, revenue, servicing cost and reliability · gold.fact_transaction")
    sw = (p.width - 32 - 3 * 8 - 300) / 4
    for i, (f, t) in enumerate([(C("dim_date", "year"), "Year"), (C("dim_bank", "bank_name"), "Bank"),
                                (C(F, "channel"), "Channel"), (C(F, "transaction_type"), "Transaction type")]):
        p.add(f"slicer{i}", round(16 + i * (sw + 8), 1), 72, round(sw, 1), 52,
              slicer(f, t, sync="Year" if i == 0 else None))
    p.add("period", p.width - 16 - 292, 72, 292, 52, visual(
        "card", {"Values": [proj(M("Data Period"))]},
        objects={"labels": props(fontSize=lit(11.0), color=color(MUTED)),
                 "categoryLabels": props(show=lit(False))},
        vco=container_objects(None)))
    kpi_row(p, 132, [
        (M("Transactions"), "Total transactions"),
        (M("Transaction Value (EGP)"), "Completed value (EGP)"),
        (M("Fee Revenue (EGP)"), "Fee revenue (EGP)"),
        (M("Channel Cost (EGP)"), "Servicing cost (EGP, est.)"),
        (M("Avg Transaction Value (EGP)"), "Avg transaction value (EGP)"),
        (M("Failure Rate %"), "Failure rate"),
        (M("Transactions YoY % (Latest Year)"), M("Title Volume YoY")),
        (M("Transaction Value YoY % (Latest Year)"), M("Title Value YoY")),
    ])
    y2, h2 = 232, 250
    p.add("monthly", 16, y2, 620, h2, visual(
        "lineClusteredColumnComboChart",
        {"Category": [proj(C("dim_date", "year_month"), "Month")],
         "Y": [proj(M("Transactions"))],
         "Y2": [proj(M("Transaction Value (EGP)"), "Completed value (EGP)")]},
        sort=[(C("dim_date", "year_month"), "Ascending")],
        objects=chart_objects(labels=False, legend=True),
        vco=container_objects("Monthly transaction volume and completed value")))
    p.add("yoy", 644, y2, 320, h2, visual(
        "lineClusteredColumnComboChart",
        {"Category": [proj(C("dim_date", "year"), "Year")],
         "Y": [proj(M("Transaction Value (EGP)"), "Completed value (EGP)")],
         "Y2": [proj(M("Transaction Value YoY %"), "Value YoY %")]},
        sort=[(C("dim_date", "year"), "Ascending")],
        objects=chart_objects(labels=True, legend=True),
        vco=container_objects("Year-over-year growth")))
    p.add("status", 972, y2, 292, h2, visual(
        "donutChart",
        {"Category": [proj(C(F, "transaction_status"), "Status")], "Y": [proj(M("Transactions"))]},
        objects={"labels": props(show=lit(True), labelStyle=lit("Percent of total")),
                 "legend": props(show=lit(True), position=lit("Bottom"))},
        vco=container_objects("Transaction status breakdown")))
    y3, h3 = 490, 238
    p.add("banks", 16, y3, 616, h3, visual(
        "tableEx",
        {"Values": [proj(C("dim_bank", "bank_name"), "Bank"), proj(C("dim_bank", "bank_type"), "Type"),
                    proj(M("Transactions")), proj(M("Transaction Value (EGP)"), "Completed value (EGP)"),
                    proj(M("Fee Revenue (EGP)"), "Fee revenue (EGP)"),
                    proj(M("Channel Cost (EGP)"), "Channel cost (EGP)"),
                    proj(M("Failure Rate %"), "Failure rate"), proj(M("Digital Share %"), "Digital share")]},
        sort=[(M("Transaction Value (EGP)"), "Descending")],
        vco=container_objects("Bank performance")))
    p.add("branches", 640, y3, 624, h3, visual(
        "tableEx",
        {"Values": [proj(C("dim_branch", "branch_name"), "Branch"),
                    proj(C("dim_branch", "branch_governorate"), "Governorate"),
                    proj(M("Transactions")), proj(M("Transaction Value (EGP)"), "Completed value (EGP)"),
                    proj(M("Fee Revenue (EGP)"), "Fee revenue (EGP)"),
                    proj(M("Failure Rate %"), "Failure rate")]},
        sort=[(M("Transaction Value (EGP)"), "Descending")],
        vco=container_objects("Branch performance", subtitle="Right-click a branch → Drill through → Branch Detail")))
    footnote(p, "Value = completed transactions in EGP (amount_egp). Fees converted to EGP at each row's own rate. "
                "Channel cost = estimated_channel_cost_egp (estimate, ≈ EGP 16 assisted / EGP 1 digital). "
                "Growth cards compare the latest year in the selection with the year before.")
    return p


def page_digital() -> Page:
    p = Page("DigitalAdoption", "Digital Adoption & Cost Optimization")
    header(p, "Digital Adoption & Cost Optimization",
           "Digital vs assisted channels, who has moved, and the estimated saving of moving the rest")
    sw = (p.width - 32 - 4 * 8) / 5
    for i, (f, t) in enumerate([(C("dim_date", "year"), "Year"), (C("dim_branch", "branch_region"), "Branch region"),
                                (C("dim_customer", "customer_segment"), "Customer segment"),
                                (C("dim_customer", "age_band"), "Age band"),
                                (C("dim_customer", "income_band"), "Income quintile")]):
        p.add(f"slicer{i}", round(16 + i * (sw + 8), 1), 72, round(sw, 1), 52,
              slicer(f, t, sync="Year" if i == 0 else None))
    kpi_row(p, 132, [
        (M("Digital Share %"), "Digital share of transactions"),
        (M("Digital Adoption Rate %"), "Digital adoption (active customers)"),
        (M("Assisted Transactions"), "Assisted transactions"),
        (M("Migration Opportunities"), "Eligible assisted transactions"),
        (M("Potential Cost Savings (EGP)"), "Est. potential savings (EGP)"),
        (M("Digital Share if Migrated %"), "Digital share if migrated (target)"),
        (M("Assisted vs Digital Cost Multiple"), "Assisted vs digital cost"),
        (M("Fee Coverage of Channel Cost %"), "Fee coverage of channel cost"),
    ])
    y2, h2 = 232, 250
    p.add("bychannel", 16, y2, 380, h2, visual(
        "clusteredBarChart",
        {"Category": [proj(C(F, "channel"), "Channel")], "Y": [proj(M("Transactions"))],
         "Series": [proj(C(F, "channel_group"), "Channel group")]},
        sort=[(M("Transactions"), "Descending")],
        objects=chart_objects(labels=True, legend=True),
        vco=container_objects("Transaction volume by channel", tooltip_page=TOOLTIP_PAGE)))
    p.add("trend", 404, y2, 460, h2, visual(
        "lineChart",
        {"Category": [proj(C("dim_date", "year_month"), "Month")],
         "Y": [proj(M("Digital Share %"), "Digital share"),
               proj(M("Digital Adoption Rate %"), "Digital adoption rate")]},
        sort=[(C("dim_date", "year_month"), "Ascending")],
        objects=chart_objects(labels=False, legend=True),
        vco=container_objects("Digital adoption by month",
                              subtitle="Adoption = customers with ≥1 digital transaction in the month / active customers")))
    p.add("segment", 872, y2, 392, h2, visual(
        "hundredPercentStackedColumnChart",
        {"Category": [proj(C("dim_customer", "customer_segment"), "Segment")],
         "Y": [proj(M("Transactions"))],
         "Series": [proj(C(F, "channel_group"), "Channel group")]},
        objects=chart_objects(labels=True, legend=True),
        vco=container_objects("Digital vs assisted share by customer segment")))
    y3, h3 = 490, 238
    p.add("age", 16, y3, 250, h3, visual(
        "clusteredColumnChart",
        {"Category": [proj(C("dim_customer", "age_band"), "Age band")],
         "Y": [proj(M("Digital Share %"), "Digital share")],
         "Tooltips": [proj(M("Digital Adoption Rate %")), proj(M("Active Customers"))]},
        sort=[(C("dim_customer", "age_band"), "Ascending")],
        objects=chart_objects(labels=True),
        vco=container_objects("Digital share by age band")))
    p.add("income", 274, y3, 280, h3, visual(
        "clusteredColumnChart",
        {"Category": [proj(C("dim_customer", "income_band"), "Income quintile")],
         "Y": [proj(M("Digital Share %"), "Digital share")],
         "Tooltips": [proj(M("Digital Adoption Rate %")), proj(M("Active Customers"))]},
        sort=[(C("dim_customer", "income_band"), "Ascending")],
        objects=chart_objects(labels=True),
        vco=container_objects("Digital share by income quintile")))
    p.add("migration", 562, y3, 300, h3, visual(
        "clusteredBarChart",
        {"Category": [proj(C(F, "transaction_type"), "Transaction type")],
         "Y": [proj(M("Migration Opportunities"), "Eligible assisted transactions")],
         "Series": [proj(C(F, "channel"), "Channel")],
         "Tooltips": [proj(M("Potential Cost Savings (EGP)"), "Est. potential savings (EGP)")]},
        sort=[(M("Migration Opportunities"), "Descending")],
        objects=chart_objects(labels=False, legend=True),
        vco=container_objects("Eligible assisted txns by type")))
    p.add("branches", 870, y3, 394, h3, visual(
        "tableEx",
        {"Values": [proj(C("dim_branch", "branch_name"), "Branch"),
                    proj(M("Digital Share %"), "Digital share"),
                    proj(M("Migration Opportunities"), "Eligible txns"),
                    proj(M("Potential Cost Savings (EGP)"), "Est. savings (EGP)")]},
        sort=[(M("Potential Cost Savings (EGP)"), "Descending")],
        vco=container_objects("Digital adoption by branch", subtitle="Drill through for branch detail")))
    footnote(p, "ESTIMATES, NOT REALIZED SAVINGS. Eligible = transfers, bill payments and loan repayments made at ATM, "
                "branch or call center (digital_migration_opportunity). Savings = their estimated channel cost minus the same "
                "count at the digital cost per transaction (≈ EGP 16 → EGP 1). 'Assisted' also includes System and Other channels.")
    return p


def page_dq() -> Page:
    p = Page("DataQuality", "Data Quality & Pipeline Monitoring")
    header(p, "Data Quality & Pipeline Monitoring",
           "Validation, automatic repair, quarantine and correction rounds · semantic.dq_*")
    sw = (p.width - 32 - 3 * 8) / 4
    for i, (f, t) in enumerate([(C("dq_runs", "run_id"), "Run"), (C("dq_runs", "dataset"), "Dataset"),
                                (C("dq_rule_checks", "dq_dimension"), "DQ dimension (rule visuals)"),
                                (C("dq_rule_checks", "severity"), "Severity (rule visuals)")]):
        p.add(f"slicer{i}", round(16 + i * (sw + 8), 1), 72, round(sw, 1), 52, slicer(f, t))
    D, R = "dq_runs", "dq_rule_checks"
    kpi_row(p, 132, [
        (M("Initial DQ Score %", D), "Initial DQ score"),
        (M("Final DQ Score %", D), "Final DQ score"),
        (M("DQ Score Uplift (pp)", D), "DQ score improvement (pp)"),
        (M("Processed Records", D), "Records processed"),
        (M("Records with Issues", D), "Records with issues"),
        (M("Auto-Fix Rate %", D), "Automatic repair rate"),
        (M("Quarantine Resolution Rate %", D), "Quarantine resolution rate"),
        (M("Unresolved Records", D), "Unresolved records"),
    ])
    y2, h2 = 232, 250
    p.add("outcomes", 16, y2, 380, h2, visual(
        "clusteredBarChart",
        {"Y": [proj(M("Valid Records", D), "Valid on first pass"),
               proj(M("Auto-Fixed Records", D), "Auto-fixed"),
               proj(M("Quarantined Records", D), "Quarantined"),
               proj(M("Resolved Records", D), "Resolved after quarantine"),
               proj(M("Unresolved Records", D), "Still unresolved")]},
        objects=chart_objects(labels=True, legend=True),
        vco=container_objects("Record outcomes",
                              subtitle="Auto-fixed records are also counted as valid")))
    p.add("dimension", 404, y2, 480, h2, visual(
        "lineClusteredColumnComboChart",
        {"Category": [proj(C(R, "dq_dimension"), "DQ dimension")],
         "Y": [proj(M("Values Failed", R), "Values failed")],
         "Y2": [proj(M("Rule Repair Rate %", R), "Repair rate")],
         "Tooltips": [proj(M("Rule Failure Rate %", R), "Failure rate"), proj(M("Values Checked", R))]},
        sort=[(M("Values Failed", R), "Descending")],
        objects=chart_objects(labels=True, legend=True),
        vco=container_objects("Rule failures and repair rate by DQ dimension")))
    p.add("severity", 892, y2, 372, h2, visual(
        "donutChart",
        {"Category": [proj(C(R, "severity"), "Severity")], "Y": [proj(M("Values Failed", R), "Values failed")]},
        objects={"labels": props(show=lit(True), labelStyle=lit("Category, percent of total")),
                 "legend": props(show=lit(True), position=lit("Bottom"))},
        vco=container_objects("Failed values by severity")))
    y3 = 490
    p.add("rules", 16, y3, 640, 238, visual(
        "tableEx",
        {"Values": [proj(C(R, "rule_id"), "Rule"), proj(C(R, "rule_name"), "Name"),
                    proj(C(R, "dq_dimension"), "Dimension"), proj(C(R, "severity"), "Severity"),
                    proj(M("Values Failed", R), "Failed"), proj(M("Rule Failure Rate %", R), "Failure rate"),
                    proj(M("Values Repaired", R), "Repaired"), proj(M("Rule Repair Rate %", R), "Repair rate")]},
        sort=[(M("Values Failed", R), "Descending")],
        vco=container_objects("Validation results by rule (first validation round)")))
    RR = "dq_run_rounds"
    p.add("rounds", 664, y3, 600, 118, visual(
        "tableEx",
        {"Values": [proj(C(RR, "run_id"), "Run"), proj(C(RR, "round"), "Round"), proj(C(RR, "round_type"), "Type"),
                    proj(C(RR, "finished_at"), "Finished"), proj(C(RR, "round_revalidated"), "Revalidated"),
                    proj(C(RR, "round_resolved"), "Resolved"), proj(C(RR, "round_failed_again"), "Failed again"),
                    proj(C(RR, "unresolved_records"), "Unresolved"), proj(C(RR, "final_dq_score"), "Final DQ score")]},
        sort=[(C(RR, "finished_at"), "Ascending")],
        vco=container_objects("Pipeline run history (validation and correction rounds)")))
    p.add("corrections", 664, y3 + 126, 600, 112, visual(
        "clusteredBarChart",
        {"Category": [proj(C("dq_correction_summary", "outcome"), "Outcome")],
         "Y": [proj(M("Corrections", "dq_correction_summary"))],
         "Series": [proj(C("dq_correction_summary", "correction_type"), "Correction type")]},
        sort=[(M("Corrections", "dq_correction_summary"), "Descending")],
        objects=chart_objects(labels=True, legend=True),
        vco=container_objects("Correction outcomes")))
    footnote(p, "Scores as in dbt MetricFlow: initial = valid / processed; final = (valid + resolved) / processed. "
                "Automatic repair rate = auto-fixed / records with issues. Rule figures come from the first validation round; "
                "dimension and severity slicers filter the rule visuals only.")
    return p


def page_branch_detail() -> Page:
    p = Page(DRILL_PAGE, "Branch Detail", type="Drillthrough", visibility="HiddenInViewMode")
    filt = vid("filter", DRILL_PAGE, "branch_name")
    p.extra["filterConfig"] = {"filters": [{"name": filt, "field": C("dim_branch", "branch_name"),
                                            "type": "Categorical", "howCreated": "Drillthrough"}]}
    p.extra["pageBinding"] = {"name": vid("binding", DRILL_PAGE), "type": "Drillthrough",
                              "parameters": [{"name": vid("param", DRILL_PAGE), "boundFilter": filt,
                                              "fieldExpr": C("dim_branch", "branch_name")}]}
    p.add("header-bg", 0, 0, p.width, 64, textbox([
        ("Branch Detail", {"fontFamily": "Segoe UI Semibold", "fontSize": "20pt", "color": "#FFFFFF"}),
        ("Drill-through from Executive Overview or Digital Adoption", {"fontFamily": "Segoe UI", "fontSize": "10pt", "color": "#BFD7EA"}),
    ], bg=NAVY))
    p.add("back", p.width - 140, 14, 124, 36, {
        "visualType": "actionButton",
        "objects": {
            "icon": [{"properties": {"show": lit(True), "shapeType": lit("back"), "lineColor": color("#FFFFFF"),
                                     "lineWeight": lit(2.0)}, "selector": {"id": "default"}}],
            "text": [{"properties": {"show": lit(True), "text": lit("Back"), "fontColor": color("#FFFFFF"),
                                     "fontSize": lit(11.0)}, "selector": {"id": "default"}}],
            "fill": [{"properties": {"show": lit(True), "fillColor": color(TEAL), "transparency": lit(0.0)},
                      "selector": {"id": "default"}}],
            "outline": [{"properties": {"show": lit(False)}, "selector": {"id": "default"}}]},
        "visualContainerObjects": {"visualLink": props(show=lit(True), type=lit("Back")),
                                   "title": props(show=lit(False)),
                                   "background": props(show=lit(False)),
                                   "border": props(show=lit(False))},
        "drillFilterOtherVisuals": True})
    for i, (m, w) in enumerate([("Selected Branch", 400), ("Selected Branch Location", 832)]):
        p.add(f"branch{i}", 16 if i == 0 else 424, 72, w, 52, visual(
            "card", {"Values": [proj(M(m))]},
            objects={"labels": props(fontSize=lit(16.0 if i == 0 else 12.0), color=color(NAVY if i == 0 else MUTED)),
                     "categoryLabels": props(show=lit(False))},
            vco=container_objects(None)))
    kpi_row(p, 132, [
        (M("Transactions"), "Transactions"),
        (M("Transaction Value (EGP)"), "Completed value (EGP)"),
        (M("Fee Revenue (EGP)"), "Fee revenue (EGP)"),
        (M("Channel Cost (EGP)"), "Channel cost (EGP, est.)"),
        (M("Failure Rate %"), "Failure rate"),
        (M("Digital Share %"), "Digital share"),
        (M("Migration Opportunities"), "Eligible assisted txns"),
        (M("Potential Cost Savings (EGP)"), "Est. potential savings (EGP)"),
    ])
    p.add("monthly", 16, 232, 820, 250, visual(
        "lineClusteredColumnComboChart",
        {"Category": [proj(C("dim_date", "year_month"), "Month")], "Y": [proj(M("Transactions"))],
         "Y2": [proj(M("Digital Share %"), "Digital share")]},
        sort=[(C("dim_date", "year_month"), "Ascending")],
        objects=chart_objects(labels=False, legend=True),
        vco=container_objects("Monthly volume and digital share")))
    p.add("status", 844, 232, 420, 250, visual(
        "donutChart",
        {"Category": [proj(C(F, "transaction_status"), "Status")], "Y": [proj(M("Transactions"))]},
        objects={"labels": props(show=lit(True), labelStyle=lit("Percent of total")),
                 "legend": props(show=lit(True), position=lit("Bottom"))},
        vco=container_objects("Transaction status")))
    p.add("channel", 16, 490, 620, 238, visual(
        "clusteredBarChart",
        {"Category": [proj(C(F, "channel"), "Channel")], "Y": [proj(M("Transactions"))],
         "Series": [proj(C(F, "channel_group"), "Channel group")]},
        sort=[(M("Transactions"), "Descending")],
        objects=chart_objects(labels=True, legend=True),
        vco=container_objects("Volume by channel", tooltip_page=TOOLTIP_PAGE)))
    p.add("segment", 644, 490, 620, 238, visual(
        "hundredPercentStackedColumnChart",
        {"Category": [proj(C("dim_customer", "customer_segment"), "Segment")], "Y": [proj(M("Transactions"))],
         "Series": [proj(C(F, "channel_group"), "Channel group")]},
        objects=chart_objects(labels=True, legend=True),
        vco=container_objects("Digital vs assisted by customer segment")))
    footnote(p, "All figures are for the branch passed by drill-through, within the filters of the page it came from.")
    return p


def page_tooltip() -> Page:
    p = Page(TOOLTIP_PAGE, "Channel Tooltip", width=320, height=240, type="Tooltip", visibility="HiddenInViewMode")
    p.extra["pageBinding"] = {"name": vid("binding", TOOLTIP_PAGE), "type": "Tooltip"}
    cards = [(M("Transactions"), "Transactions"), (M("Cost per Transaction (EGP)"), "Cost per transaction (est.)"),
             (M("Failure Rate %"), "Failure rate"), (M("Fee Revenue (EGP)"), "Fee revenue (EGP)")]
    for i, (m, t) in enumerate(cards):
        p.add(f"kpi{i}", 8 + (i % 2) * 156, 8 + (i // 2) * 116, 148, 108, card(m, t))
    return p


THEME = {
    "name": "Transight Banking",
    "dataColors": [NAVY, TEAL, "#3E7CB1", "#8DA9C4", "#F2A541", "#D1495B", "#5C6B73", "#7FD1C7",
                   "#1F4E79", "#A3C9A8"],
    "foreground": INK, "foregroundNeutralSecondary": MUTED, "foregroundNeutralTertiary": "#8A97A6",
    "background": "#FFFFFF", "backgroundLight": CANVAS, "backgroundNeutral": LINE,
    "tableAccent": TEAL, "good": TEAL, "neutral": "#F2A541", "bad": "#D1495B",
    "maximum": NAVY, "center": "#8DA9C4", "minimum": "#E8F1F8",
    "textClasses": {
        "callout": {"fontSize": 22, "fontFace": "Segoe UI Semibold", "color": NAVY},
        "title": {"fontSize": 11, "fontFace": "Segoe UI Semibold", "color": NAVY},
        "header": {"fontSize": 11, "fontFace": "Segoe UI Semibold", "color": NAVY},
        "label": {"fontSize": 9, "fontFace": "Segoe UI", "color": INK},
    },
    "visualStyles": {
        "*": {"*": {
            "background": [{"show": True, "color": {"solid": {"color": "#FFFFFF"}}, "transparency": 0}],
            "border": [{"show": True, "color": {"solid": {"color": LINE}}, "radius": 8}],
            "dropShadow": [{"show": False}],
            "title": [{"show": True, "fontColor": {"solid": {"color": NAVY}}, "fontSize": 11,
                       "fontFamily": "Segoe UI Semibold"}],
            "subTitle": [{"fontColor": {"solid": {"color": MUTED}}, "fontSize": 8}],
            "padding": [{"top": 6, "bottom": 6, "left": 10, "right": 10}],
            "visualHeader": [{"background": {"solid": {"color": "#FFFFFF"}}}],
        }},
        "page": {"*": {
            "background": [{"color": {"solid": {"color": CANVAS}}, "transparency": 0}],
            "outspace": [{"color": {"solid": {"color": CANVAS}}, "transparency": 0}],
        }},
        "card": {"*": {
            "labels": [{"color": {"solid": {"color": NAVY}}, "fontSize": 20, "fontFamily": "Segoe UI Semibold"}],
            "categoryLabels": [{"show": False}],
            "title": [{"fontSize": 9, "fontColor": {"solid": {"color": MUTED}}, "fontFamily": "Segoe UI"}],
            "border": [{"show": True, "color": {"solid": {"color": LINE}}, "radius": 8}],
        }},
        "tableEx": {"*": {
            "columnHeaders": [{"fontColor": {"solid": {"color": "#FFFFFF"}},
                               "backColor": {"solid": {"color": NAVY}}, "fontFamily": "Segoe UI Semibold"}],
            "values": [{"fontColorPrimary": {"solid": {"color": INK}}, "backColorPrimary": {"solid": {"color": "#FFFFFF"}},
                        "backColorSecondary": {"solid": {"color": "#F4F7FA"}}}],
            "total": [{"fontColor": {"solid": {"color": NAVY}}}],
        }},
        "slicer": {"*": {"header": [{"fontColor": {"solid": {"color": NAVY}}, "textSize": 9}]}},
    },
}


def build_report(base: Path) -> None:
    rp = base / f"{NAME}.Report"
    d = rp / "definition"
    (d / "pages").mkdir(parents=True)
    write_json(rp / "definition.pbir", {
        "$schema": f"{SCHEMA}/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {"byPath": {"path": f"../{NAME}.SemanticModel"}},
    })
    write_json(d / "version.json", {
        "$schema": f"{SCHEMA}/item/report/definition/versionMetadata/1.0.0/schema.json",
        "version": "2.0.0"})
    theme_name = "TransightBanking.json"
    res = rp / "StaticResources" / "RegisteredResources"
    res.mkdir(parents=True)
    write_json(res / theme_name, THEME)
    write_json(d / "report.json", {
        "$schema": f"{SCHEMA}/item/report/definition/report/3.0.0/schema.json",
        "themeCollection": {"customTheme": {
            "name": theme_name, "type": "RegisteredResources",
            "reportVersionAtImport": {"visual": "2.4.0", "report": "3.0.0", "page": "2.0.0"}}},
        "resourcePackages": [{"name": "RegisteredResources", "type": "RegisteredResources",
                              "items": [{"name": theme_name, "path": theme_name, "type": "CustomTheme"}]}],
        "settings": {"useStylableVisualContainerHeader": True, "exportDataMode": "AllowSummarized",
                     "defaultDrillFilterOtherVisuals": True, "allowChangeFilterTypes": True,
                     "useEnhancedTooltips": True},
    })
    pages = [page_executive(), page_digital(), page_dq(), page_branch_detail(), page_tooltip()]
    write_json(d / "pages" / "pages.json", {
        "$schema": f"{SCHEMA}/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [p.name for p in pages], "activePageName": pages[0].name})
    for p in pages:
        pd = d / "pages" / p.name
        page = {"$schema": PAGE_SCHEMA, "name": p.name, "displayName": p.display,
                "displayOption": "FitToPage", "height": p.height, "width": p.width}
        page.update(p.extra)
        write_json(pd / "page.json", page)
        for v in p.visuals:
            write_json(pd / "visuals" / v["name"] / "visual.json", v)


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true", help="replace an existing generated project")
    args = ap.parse_args()
    targets = [OUT / f"{NAME}.pbip", OUT / f"{NAME}.SemanticModel", OUT / f"{NAME}.Report"]
    if any(t.exists() for t in targets):
        if not args.force:
            print(f"{OUT} already holds the project; use --force to regenerate it "
                  "(edits made in Power BI Desktop would be lost).", file=sys.stderr)
            return 1
        for t in targets:
            if t.is_dir():
                shutil.rmtree(t)
            elif t.exists():
                t.unlink()
    measures = build_model(OUT)
    build_report(OUT)
    write_json(OUT / f"{NAME}.pbip", {
        "$schema": f"{SCHEMA}/pbip/pbipProperties/1.0.0/schema.json",
        "version": "1.0",
        "artifacts": [{"report": {"path": f"{NAME}.Report"}}],
        "settings": {"enableAutoRecovery": True},
    })
    for t, names in measures.items():
        if names:
            print(f"{t}: {len(names)} measures")
    print(f"written: {OUT / (NAME + '.pbip')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
