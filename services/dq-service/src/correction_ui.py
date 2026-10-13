"""Correction UI: fix quarantined records without knowing the rules.

    streamlit run services/dq-service/src/correction_ui.py
    python dq.py ui

Reads and writes the dq tables in PostgreSQL (layout.py); the database
comes from the POSTGRES_* variables, as for every other step.

Pages
    Fix records    pick a problem in plain words ("Missing email"); accept
                   the values found on other records of the same customer
                   or account for many records at once, or go through the
                   records one by one (each problem explained, with an input
                   that only accepts valid values); records held because
                   many values were changed automatically show each change
                   to accept (ACCEPT), correct or remove
    My changes     everything you changed; check it and send it
    Progress       what is fixed, removed, still waiting; "Publish to clean
                   data" triggers the dq_publish DAG with Airflow (merge,
                   metrics, metadata, gold; the UI runs no Spark for it and
                   the button waits while a load, correction file or
                   publish is running). Without Airflow it runs the merge
                   (merge.py) and the metric tables (python dq.py metrics)
                   here
    Upload a file  download the list of problems, fill it in (e.g. in
                   Excel), upload it; it is checked before anything is sent.
                   With Airflow (AIRFLOW_API_URL, airflow_trigger.py),
                   sending saves the file under data/corrections/ and
                   triggers the dq_apply_corrections DAG with it
    Load new data  upload a new source file (CSV); it is saved under
                   data/raw/uploads/ and triggers the dq_ingest_validate DAG
                   (ingest, validate, incremental load into silver, gold).
                   A file already loaded (same SHA-256) is refused. Shown
                   even before any data is loaded (the initial load)

Nothing is written until "Send" on My changes. Sending calls the same
functions as "quarantine.py import" and "quarantine.py revalidate", so a
change made here and the same change in an uploaded file give identical
results. Wording and suggestions come from correction_assist.py.
"""

from __future__ import annotations

import argparse
import csv
import getpass
import hashlib
import io
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

import airflow_trigger
import db
from correction_assist import (
    REPAIR_WORDS,
    Change,
    FieldInput,
    Suggestions,
    Wording,
    changes_to_lines,
    check_value,
    column_label,
    explain_suggestion,
    field_input,
    lower_label,
)
from dq_engine import DEFAULT_RULES, PROJECT_ROOT as ROOT
from merge import merge_run
from layout import (
    Layout,
    current_round,
    data_columns,
    list_runs,
    load_status,
)
from quarantine import (
    DEFAULT_MAX_ATTEMPTS,
    NULL_MARKER,
    build_template,
    import_corrections,
    open_errors,
    open_ids,
    read_correction_file,
    revalidate,
)
from registry import Registry, load_registry


# Shown under a record's title, when the dataset has these columns.
SUMMARY_COLUMNS = [
    "transaction_date", "transaction_type", "transaction_amount", "currency",
    "customer_first_name", "customer_last_name", "customer_id", "account_id",
]
# Searched by "Find a record".
SEARCH_COLUMNS = [
    "transaction_id", "customer_id", "account_id",
    "customer_first_name", "customer_last_name",
]
DISCARD_REASONS = [
    "It is a copy of another record",
    "It is a test or invalid record",
    "Other reason",
]
ALL_PROBLEMS = "All problems"


@dataclass
class Context:
    args: argparse.Namespace
    registry: Registry
    wording: Wording
    layout: Layout
    user: str
    max_attempts: int


# ============================================================
# Cached data (refreshed when the ledger changes)
# ============================================================

@st.cache_resource
def get_registry(rules: str) -> Registry:
    return load_registry(Path(rules))


@st.cache_resource
def get_wording(rules: str) -> Wording:
    return Wording(get_registry(rules))


@st.cache_resource(show_spinner="Preparing suggestions (first time only)...")
def get_suggestions(rules: str, layout: Layout, stamp: str) -> Suggestions:
    registry = get_registry(rules)
    columns = [c for c in Suggestions.columns_needed(registry) if c in layout.columns()]
    return Suggestions(registry, layout.load_valid(columns).drop(columns="record_id"))


def ledger_stamp(layout: Layout) -> int:
    return layout.ledger_stamp()


@st.cache_data(show_spinner=False)
def read_status(layout: Layout, stamp: int) -> pd.DataFrame:
    return load_status(layout)


@st.cache_data(show_spinner="Loading the records on hold...")
def read_round(
    layout: Layout, round_number: int, rules: str, stamp: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Open errors (with plain group titles) and open records."""

    registry = get_registry(rules)
    status = load_status(layout)
    errors = get_wording(rules).add_groups(open_errors(layout, status, round_number))

    actions = registry.severity_actions
    errors["blocking"] = errors["severity"].map(actions).eq("quarantine")

    records = layout.load_quarantine(round_number)
    records = records[records["record_id"].isin(open_ids(status, round_number))]
    return errors, records.set_index("record_id")


@st.cache_data(show_spinner=False)
def read_repairs(
    layout: Layout, round_number: int, rules: str, stamp: int,
) -> pd.DataFrame:
    """Automatic repairs of the open records held for review."""

    errors, _ = read_round(layout, round_number, rules, stamp)
    held = set(errors.loc[errors["error_category"].eq("auto_fix_review"), "record_id"])
    if not held:
        return pd.DataFrame(columns=["record_id", "column", "original_value",
                                     "cleaned_value", "action"])
    return layout.load_audit(record_ids=held, outcome="remediated")


def repairs_table(rows: pd.DataFrame) -> pd.DataFrame:
    def shown(value: object) -> str:
        return "(empty)" if pd.isna(value) else f"'{value}'"

    return pd.DataFrame({
        "Field": [column_label(c) for c in rows["column"]],
        "Before": [shown(v) for v in rows["original_value"]],
        "After": [shown(v) for v in rows["cleaned_value"]],
        "What was done": [REPAIR_WORDS.get(a, a) for a in rows["action"]],
    })


@st.cache_data(show_spinner="Looking for values on other records...")
def suggest_round(
    layout: Layout, round_number: int, rules: str, stamp: int, valid_stamp: str,
) -> tuple[pd.DataFrame, list[str]]:
    """Suggestions for every missing blocking field, and the records whose
    blocking problems can all be solved by them."""

    errors, records = read_round(layout, round_number, rules, stamp)
    suggestions = get_suggestions(rules, layout, valid_stamp)

    blocking = errors[errors["blocking"]]
    missing = blocking[blocking["error_category"].eq("null") & blocking["column"].notna()]
    if not len(missing):
        return pd.DataFrame(), []

    needs = pd.crosstab(missing["record_id"], missing["column"]).gt(0)
    found = suggestions.for_records(records.loc[needs.index], needs)

    found_count = found.groupby(level=0).size()
    needed_count = needs.sum(axis=1)
    other_problems = set(
        blocking.loc[~blocking.index.isin(missing.index), "record_id"]
    )
    complete = [
        record for record in needs.index
        if found_count.get(record, 0) == needed_count[record]
        and record not in other_problems
    ]
    return found, complete


# ============================================================
# Changes kept in the session until they are sent
# ============================================================

def my_changes(layout: Layout) -> dict[str, Change]:
    store = st.session_state.setdefault("changes", {})
    return store.setdefault(layout.run_id, {})


def record_title(record_id: str, record: pd.Series, registry: Registry) -> str:
    key = (registry.dataset.get("primary_key") or [None])[0]
    if key and key in record.index and pd.notna(record[key]):
        return f"{column_label(key)} {record[key]}"
    return f"Record {record_id.rsplit('_', 1)[-1]}"


def record_summary(record: pd.Series) -> str:
    parts = []
    for column in SUMMARY_COLUMNS:
        if column in record.index:
            value = record[column]
            parts.append(f"{column_label(column)}: "
                         f"{'—' if pd.isna(value) else value}")
    return " · ".join(parts)


# ============================================================
# Sending (shared by My changes and Upload a file)
# ============================================================

def plain_refusal(reason: str) -> str:
    """The import's refusal reason in everyday words."""

    action = re.match(r"unknown action '(.*)'", reason)
    if action:
        return f"action must be FIX, DISCARD or ACCEPT (it says '{action.group(1)}')"
    if reason.endswith("is not an editable schema column"):
        return "This field does not exist or cannot be changed"
    if reason.startswith("record is not open"):
        return ("This record is not waiting to be fixed (already fixed or "
                "removed, or the record does not exist)")
    if reason.startswith("corrected more than once"):
        return "The same field has two different new values"
    return reason


def send(ctx: Context, lines: pd.DataFrame, clear_changes: bool) -> None:
    """Import the lines, then check them again; the outcome shows after a rerun."""

    log: list[str] = []
    with st.status("Saving your changes...", expanded=True) as progress:
        imported = import_corrections(ctx.layout, ctx.registry, lines, ctx.user)
        st.write(f"{len(imported.accepted)} changes saved.")

        summary = None
        if len(imported.accepted):
            progress.update(label="Checking the changed records against all the "
                            "data (this takes a minute or two)...")
            summary = revalidate(
                ctx.layout, ctx.registry, ctx.max_attempts, log=log.append,
            )
        progress.update(label="Done", state="complete")

    if clear_changes:
        my_changes(ctx.layout).clear()
    st.session_state["last_send"] = {
        "run_id": ctx.layout.run_id,
        "refused": imported.refused,
        "summary": summary,
        "log": log,
    }
    st.cache_data.clear()
    st.rerun()


def show_last_send(layout: Layout) -> None:
    sent = st.session_state.get("last_send")
    if not sent or sent["run_id"] != layout.run_id:
        return

    with st.container(border=True):
        summary = sent["summary"]
        if summary:
            counts = summary["counts"]
            removed = counts["discarded"] + counts["removed_as_duplicate"]
            st.markdown("#### Your changes were checked")
            if counts["resolved"]:
                st.success(f"{counts['resolved']:,} records are fixed. They will "
                           "be added to the clean data when someone publishes "
                           "(Progress page).")
            if counts["failed_again"]:
                st.warning(f"{counts['failed_again']:,} records still have a "
                           "problem and are back on the Fix records page.")
            if counts["manual_review_required"]:
                st.error(f"{counts['manual_review_required']:,} records failed "
                         "several times and now need an expert.")
            if removed:
                st.info(f"{removed:,} records were removed.")
        else:
            st.warning("None of the changes could be saved.")

        if len(sent["refused"]):
            st.error(f"{len(sent['refused'])} changes could not be used:")
            refused = sent["refused"]
            st.dataframe(pd.DataFrame({
                "Record": refused["record_id"].to_numpy(),
                "Field": [column_label(c) if c else "" for c in refused["column"]],
                "Value": refused["new_value"].to_numpy(),
                "Why": [plain_refusal(r) for r in refused["refused"]],
            }), hide_index=True)
        if sent["log"]:
            with st.expander("Technical details"):
                st.code("\n".join(sent["log"]))
        if st.button("Close"):
            del st.session_state["last_send"]
            st.rerun()


# ============================================================
# Field editors
# ============================================================

def field_editor(
    spec: FieldInput, key: str, start: str | None, label: str,
) -> str | None:
    """An input for one field; returns its text ('' = empty), None = not set."""

    if spec.kind == "choice":
        options = spec.options
        index = options.index(start) if start in options else None
        value = st.selectbox(label, options, index=index, key=key,
                             placeholder="Choose...", help=spec.help)
        return value

    if spec.kind == "date":
        try:
            initial = date.fromisoformat(start) if start else None
        except ValueError:
            initial = None
        value = st.date_input(
            label, value=initial, min_value=spec.low, max_value=spec.high,
            format="YYYY-MM-DD", key=key, help=spec.help,
        )
        return value.isoformat() if value else None

    placeholder = f"e.g. {spec.example}" if spec.example else ""
    value = st.text_input(label, value=start or "", key=key,
                          placeholder=placeholder, help=spec.help).strip()
    return value or None


def compare_with_others(
    record: pd.Series, key_column: str, suggestions: Suggestions,
) -> pd.DataFrame:
    """Fields where this record differs from the key's usual value."""

    rows = []
    key_value = record.get(key_column)
    for (key, column), table in suggestions.tables.items():
        if key != key_column or pd.isna(key_value) or key_value not in table.index:
            continue
        usual = table.loc[key_value]
        mine = record.get(column)
        if pd.isna(mine) or str(mine) != str(usual["value"]):
            rows.append({
                "Field": column_label(column),
                "This record": "—" if pd.isna(mine) else mine,
                "Usual value": usual["value"],
                "Seen on": f"{usual['support']} of {usual['total']} records",
            })
    return pd.DataFrame(rows)


# ============================================================
# One record
# ============================================================

def duplicate_block(
    ctx: Context, record_id: str, column: str, value: str,
    records: pd.DataFrame, changes: dict[str, Change],
) -> None:
    label = lower_label(column)
    others = records[records[column].eq(value) & (records.index != record_id)]

    st.markdown("Which one is right?")
    if not len(others):
        st.caption("The other record is already in the clean data, so keep "
                   f"this one only if you give it a different {label}.")
    else:
        mine = records.loc[record_id]
        columns = [
            c for c in data_columns(records)
            if any(str(mine[c]) != str(other[c]) for _, other in others.iterrows())
        ]
        table = pd.DataFrame(
            {"This record": mine[columns]}
            | {f"Other record ({other_id.rsplit('_', 1)[-1]})": others.loc[other_id, columns]
               for other_id in others.index}
        )
        table.index = [column_label(c) for c in columns]
        st.dataframe(table)

        keep, other = st.columns(2)
        if keep.button("Keep this one, remove the other", key=f"keep:{record_id}"):
            changes[record_id] = Change(record_id, {column: (
                value, f"Kept; the other record with this {label} was removed")})
            for other_id in others.index:
                changes[other_id] = Change(
                    other_id, discard_reason=f"Copy of another record with the same {label}",
                )
            st.rerun()
        if other.button("Keep the other one, remove this", key=f"drop:{record_id}"):
            changes[record_id] = Change(
                record_id, discard_reason=f"Copy of another record with the same {label}",
            )
            for other_id in others.index:
                changes[other_id] = Change(other_id, {column: (
                    value, f"Kept; the other record with this {label} was removed")})
            st.rerun()

    st.caption(f"If both are real, give this record a different {label} below "
               "and save.")


def repairs_block(
    ctx: Context, record_id: str, record: pd.Series, repairs: pd.DataFrame,
    sentence: str, entered: dict, problems: dict,
) -> None:
    """The automatic changes of a record held for review, to confirm or correct."""

    rows = repairs[repairs["record_id"].eq(record_id)]
    with st.container(border=True):
        st.markdown(f"**{sentence}**")
        if len(rows):
            st.dataframe(repairs_table(rows), hide_index=True)
        st.caption("If the changes are right, press **Accept** below. If one "
                   "is wrong, correct it here. If the record cannot be "
                   "trusted, remove it.")
        with st.expander("Correct one of these values"):
            for column in dict.fromkeys(rows["column"]):
                if column not in ctx.registry.columns or column in entered:
                    continue
                current = None if pd.isna(record[column]) else str(record[column])
                spec = field_input(ctx.registry, column)
                value = field_editor(
                    spec, f"rv:{ctx.layout.run_id}:{record_id}:{column}",
                    current, column_label(column),
                )
                if value is not None and value != (current or ""):
                    problem = check_value(spec, value)
                    if problem:
                        st.error(problem)
                        problems[column] = problem
                    entered[column] = (value, "Corrected after an automatic change")


def record_card(
    ctx: Context, record_id: str, errors: pd.DataFrame, records: pd.DataFrame,
    found: pd.DataFrame, suggestions: Suggestions, repairs: pd.DataFrame,
) -> bool:
    """Show one record with its problems; True when it was saved or skipped."""

    changes = my_changes(ctx.layout)
    record = records.loc[record_id]
    record_errors = errors[errors["record_id"].eq(record_id)]
    blocking = record_errors[record_errors["blocking"]]
    advisory = record_errors[~record_errors["blocking"]]
    earlier = changes.get(record_id)

    st.markdown(f"### {record_title(record_id, record, ctx.registry)}")
    st.caption(record_summary(record))
    if earlier:
        st.info(f"Already in your changes: {earlier.summary()}. Saving again "
                "replaces it.")

    record_found = found.loc[[record_id]] if record_id in found.index else found.iloc[0:0]
    suggested = {
        row.column: row for row in record_found.itertuples()
    } if len(record_found) else {}

    # ---- What has to be fixed ----
    st.markdown("#### What needs fixing")
    entered: dict[str, tuple[str, str]] = {}
    problems: dict[str, str] = {}
    duplicates_shown = False
    review = blocking["error_category"].eq("auto_fix_review").any()

    for column, column_errors in blocking.groupby(blocking["column"].fillna(""), sort=False):
        sentences = " · ".join(dict.fromkeys(
            ctx.wording.problem_text(row) for _, row in column_errors.iterrows()
        ))
        if not column:
            if column_errors["error_category"].eq("auto_fix_review").any():
                repairs_block(ctx, record_id, record, repairs, sentences,
                              entered, problems)
            else:
                st.markdown(f"- {sentences}")
            continue

        with st.container(border=True):
            st.markdown(f"**{sentences}**")
            if column_errors["error_category"].eq("duplicate").any():
                duplicate_block(ctx, record_id, column, record[column],
                                records, changes)
                duplicates_shown = True

            spec = field_input(ctx.registry, column, suggestions.examples.get(column))
            suggestion = suggested.get(column)
            start = (
                earlier.fixes[column][0] if earlier and column in earlier.fixes
                else suggestion.value if suggestion is not None
                else None if column_errors["error_category"].eq("null").all()
                or column_errors["error_category"].eq("duplicate").any()
                else record[column]
            )
            if suggestion is not None:
                st.caption(f"Suggested: **{suggestion.value}**. " + explain_suggestion(
                    suggestion.source_column, suggestion.source_value,
                    suggestion.support, suggestion.total,
                ) + ".")

            value = field_editor(
                spec, f"v:{ctx.layout.run_id}:{record_id}:{column}", start,
                f"New {lower_label(column)}",
            )
            if value is not None:
                problem = check_value(spec, value)
                if problem:
                    st.error(problem)
                    problems[column] = problem
                reason = (
                    "Suggested value: " + explain_suggestion(
                        suggestion.source_column, suggestion.source_value,
                        suggestion.support, suggestion.total)
                    if suggestion is not None and value == suggestion.value
                    else "Entered by hand"
                )
                entered[column] = (value, reason)

    # ---- Not blocking ----
    if len(advisory):
        with st.expander(f"Also worth checking ({len(advisory)}; these do not "
                         "keep the record on hold)"):
            for _, row in advisory.iterrows():
                st.markdown(f"- {ctx.wording.problem_text(row)}")
                if row["error_category"] == "consistency" and pd.notna(row["column"]):
                    table = compare_with_others(record, row["column"], suggestions)
                    if len(table):
                        st.dataframe(table, hide_index=True)

    with st.expander("Change another field"):
        editable = [c for c, s in ctx.registry.columns.items()
                    if not s.get("generated") and c not in blocking["column"].values]
        column = st.selectbox(
            "Field", editable, index=None, format_func=column_label,
            key=f"extra-field:{record_id}", placeholder="Choose a field...",
        )
        if column:
            st.caption(f"Now: {record[column] if pd.notna(record[column]) else '—'}")
            spec = field_input(ctx.registry, column, suggestions.examples.get(column))
            value = field_editor(spec, f"extra:{record_id}:{column}", None,
                                 f"New {lower_label(column)}")
            if value is not None:
                problem = check_value(spec, value)
                if problem:
                    st.error(problem)
                    problems[column] = problem
                entered[column] = (value, "Entered by hand")

    with st.expander("See all the values of this record"):
        failing = set(blocking["column"].dropna()) | set(advisory["column"].dropna())
        columns = data_columns(records)
        table = pd.DataFrame({
            "Field": [column_label(c) for c in columns],
            "Value": record[columns].to_numpy(),
            "Problem": ["yes" if c in failing else "" for c in columns],
        })
        st.dataframe(table, hide_index=True)

    note = st.text_input("Note (optional)", key=f"note:{record_id}",
                         placeholder="Why you made this change")

    # ---- Buttons ----
    save_col, skip_col, remove_col = st.columns([2, 1, 1])
    label = (
        "Save and go to the next record" if not review
        else "Accept and go to the next record" if not entered
        else "Accept the other changes, save my corrections"
    )
    if save_col.button(label, type="primary",
                       key=f"save:{record_id}", disabled=bool(problems)):
        if not entered and not review:
            st.error("Fill in at least one field, or skip this record.")
            return False
        still = [lower_label(c) for c in blocking["column"].dropna().unique()
                 if c not in entered and not duplicates_shown]
        if note:
            entered = {c: (v, note) for c, (v, _) in entered.items()}
        changes[record_id] = Change(
            record_id, entered,
            accept_reason=(note or "Automatic changes checked and accepted")
            if review else None,
        )
        if still:
            st.toast(f"Saved. Still missing: {', '.join(still)} - the record "
                     "will stay on hold until that is filled in.")
        return True

    if skip_col.button("Skip", key=f"skip:{record_id}"):
        return True

    with remove_col.popover("Remove this record"):
        reason = st.radio("Why?", DISCARD_REASONS, key=f"why:{record_id}")
        detail = st.text_input("Details", key=f"why-detail:{record_id}")
        if st.button("Remove it", key=f"remove:{record_id}", type="primary"):
            text = reason + (f": {detail}" if detail else "")
            changes[record_id] = Change(record_id, discard_reason=text)
            return True

    return False


# ============================================================
# Pages
# ============================================================

def page_fix(ctx: Context) -> None:
    layout = ctx.layout
    status = read_status(layout, ledger_stamp(layout))
    round_number = current_round(status)
    st.title("Fix records")
    if round_number is None:
        st.success("Nothing is waiting to be fixed.")
        return

    stamp = ledger_stamp(layout)
    errors, records = read_round(layout, round_number, str(ctx.args.rules), stamp)
    valid_stamp = layout.validated_at()
    suggestions = get_suggestions(str(ctx.args.rules), layout, valid_stamp)
    found, complete = suggest_round(
        layout, round_number, str(ctx.args.rules), stamp, valid_stamp,
    )
    changes = my_changes(layout)

    st.markdown(
        f"**{len(records):,} records** are on hold because something in them "
        "is missing or wrong. Fix them here: they are checked again when you "
        "send your changes, and the ones that pass go into the clean data at "
        "the next publish."
    )
    with st.expander("How does this work?"):
        st.markdown(
            "1. **Choose a problem** below, e.g. *Missing email*.\n"
            "2. **Use the suggestions**: many missing values are already "
            "known from other records of the same customer or account. You "
            "can accept them all at once.\n"
            "3. **Or go through the records one by one**: each problem is "
            "explained, and you fill in or choose the right value.\n"
            "4. Open **My changes** in the sidebar, check them and **send** "
            "them. Nothing is changed before you send.\n"
            "5. On **Progress**, **Publish to clean data** adds every fixed "
            "record to the clean data (e.g. once at the end of the day)."
        )

    # ---- What to fix ----
    blocking = errors[errors["blocking"] & ~errors["record_id"].isin(changes)]
    groups = blocking.groupby("group")["record_id"].nunique().sort_values(ascending=False)
    if not len(groups):
        st.success("Every record on hold is in your changes. Open "
                   "**My changes** to send them.")
        return

    options = [ALL_PROBLEMS, *groups.index]
    counts = {ALL_PROBLEMS: blocking["record_id"].nunique(), **groups.to_dict()}
    group = st.pills(
        "What do you want to fix?", options, default=ALL_PROBLEMS,
        format_func=lambda g: f"{g} ({counts.get(g, 0):,})",
        key=f"group:{layout.run_id}:{hash(tuple(options))}",
    ) or ALL_PROBLEMS

    in_group = blocking if group == ALL_PROBLEMS else blocking[blocking["group"].eq(group)]
    group_ids = list(dict.fromkeys(in_group["record_id"]))

    # ---- Quick fix with suggestions ----
    complete_ids = set(complete)
    ready = [r for r in group_ids if r in complete_ids]
    if ready:
        with st.container(border=True):
            st.markdown(f"#### Quick fix: {len(ready):,} records can be completed "
                        "with values found on other records")
            st.caption("Only records where every missing value was found are "
                       "included. They are added to My changes; nothing is sent yet.")
            sample = found.loc[found.index.isin(ready[:200])].head(10)
            st.dataframe(pd.DataFrame({
                "Record": [record_title(r, records.loc[r], ctx.registry) for r in sample.index],
                "Field": [column_label(c) for c in sample["column"]],
                "Suggested value": sample["value"].to_numpy(),
                "Where it comes from": [
                    explain_suggestion(s.source_column, s.source_value, s.support, s.total)
                    for s in sample.itertuples()
                ],
            }), hide_index=True)
            if st.button(f"Add all {len(ready):,} to my changes", type="primary"):
                chosen = found.loc[found.index.isin(ready)]
                for record_id, rows in chosen.groupby(level=0):
                    changes[record_id] = Change(record_id, {
                        row.column: (row.value, "Suggested value: " + explain_suggestion(
                            row.source_column, row.source_value, row.support, row.total))
                        for row in rows.itertuples()
                    })
                st.rerun()

    # ---- Accept automatic changes in bulk ----
    review_group = ctx.wording.group_title("auto_fix_review", None)
    repairs = read_repairs(layout, round_number, str(ctx.args.rules), stamp)
    if group == review_group:
        is_review = blocking["error_category"].eq("auto_fix_review")
        only_review = (
            set(blocking.loc[is_review, "record_id"])
            - set(blocking.loc[~is_review, "record_id"])
        )
        to_accept = [r for r in group_ids if r in only_review]
        if to_accept:
            with st.container(border=True):
                st.markdown(f"#### Accept many at once: {len(to_accept):,} records "
                            "only need their automatic changes confirmed")
                chosen = repairs[repairs["record_id"].isin(to_accept)]
                kinds = (
                    chosen["action"].map(lambda a: REPAIR_WORDS.get(a, a))
                    .value_counts().rename("Changes").rename_axis("What was done")
                )
                st.dataframe(kinds)
                st.caption("Examples:")
                st.dataframe(repairs_table(chosen.head(10)), hide_index=True)
                sure = st.checkbox("I have looked at these examples and the "
                                   "automatic changes are right")
                if st.button(f"Accept the changes of all {len(to_accept):,} records",
                             disabled=not sure):
                    for record_id in to_accept:
                        changes[record_id] = Change(
                            record_id,
                            accept_reason="Automatic changes accepted in bulk "
                            "after checking examples",
                        )
                    st.rerun()

    # ---- One by one ----
    st.markdown("#### One by one")
    search = st.text_input(
        "Find a record",
        placeholder="Name, customer ID, account ID or transaction ID",
    ).strip()
    queue = group_ids
    if search:
        columns = [c for c in SEARCH_COLUMNS if c in records.columns]
        haystack = records.loc[queue, columns].fillna("").agg(" ".join, axis=1)
        queue = list(haystack[haystack.str.contains(search, case=False, regex=False)].index)
        if not queue:
            st.info("No record on hold matches that.")
            return

    position_key = f"pos:{layout.run_id}:{group}:{search}"
    position = min(st.session_state.get(position_key, 0), len(queue) - 1)

    back, where, forward = st.columns([1, 3, 1])
    if back.button("Previous", disabled=position == 0):
        position -= 1
    if forward.button("Next", disabled=position >= len(queue) - 1):
        position += 1
    where.markdown(f"Record **{position + 1:,}** of **{len(queue):,}**")
    st.session_state[position_key] = position

    with st.container(border=True):
        done = record_card(ctx, queue[position], errors, records, found,
                           suggestions, repairs)
    if done:
        # A saved record leaves the queue, so the same position shows the next.
        if queue[position] not in changes:
            st.session_state[position_key] = min(position + 1, len(queue) - 1)
        st.rerun()


def page_changes(ctx: Context) -> None:
    st.title("My changes")
    changes = my_changes(ctx.layout)
    if not changes:
        st.info("You have no changes yet. Make them on **Fix records**.")
        return

    status = read_status(ctx.layout, ledger_stamp(ctx.layout))
    round_number = current_round(status)
    records = (
        read_round(ctx.layout, round_number, str(ctx.args.rules),
                   ledger_stamp(ctx.layout))[1]
        if round_number is not None else pd.DataFrame()
    )

    table = pd.DataFrame({
        "Undo": False,
        "Record": [
            record_title(r, records.loc[r], ctx.registry) if r in records.index else r
            for r in changes
        ],
        "Change": [change.summary() for change in changes.values()],
    }, index=list(changes))
    removed = sum(change.discard for change in changes.values())
    st.markdown(f"**{len(changes):,} records** changed"
                + (f", {removed:,} of them to be removed" if removed else "") + ".")

    edited = st.data_editor(
        table, hide_index=True, disabled=["Record", "Change"],
        column_config={"Undo": st.column_config.CheckboxColumn(width="small")},
        key=f"changes-editor:{ctx.layout.run_id}:{len(changes)}",
    )
    undo = list(edited.index[edited["Undo"]])
    if undo and st.button(f"Undo {len(undo)} selected"):
        for record_id in undo:
            changes.pop(record_id, None)
        st.rerun()

    st.divider()
    st.markdown(
        "When you send, every changed record is checked again against all the "
        "data. Records that pass are marked fixed and go into the clean data "
        "at the next **Publish** (Progress page); records that still have a "
        "problem come back to **Fix records**. This takes a minute or two."
    )
    if st.button(f"Send {len(changes):,} changes", type="primary"):
        send(ctx, changes_to_lines(list(changes.values())), clear_changes=True)


def read_publish(layout: Layout) -> dict | None:
    return layout.load_merge_result()


def when(timestamp: str) -> str:
    """'2026-10-11T18:18:17+00:00' -> '11 Oct 2026, 21:18' (local time)."""

    return datetime.fromisoformat(timestamp).astimezone().strftime("%d %b %Y, %H:%M")


def refresh_metrics() -> None:
    """Update and check the metric views (python dq.py metrics)."""

    done = subprocess.run(
        [sys.executable, str(ROOT / "dq.py"), "metrics"],
        cwd=ROOT, capture_output=True, text=True,
    )
    if done.returncode:
        raise RuntimeError("Updating the metric tables failed:\n"
                           + (done.stdout + done.stderr)[-2000:])


PUBLISH_DAG = "dq_publish"
PUBLISH_STATES = {
    "queued": ("Waiting to start", st.info),
    "running": ("Publishing: clean data, quality figures and Power BI tables...",
                st.info),
    "success": ("Published: the clean data, quality figures and Power BI "
                "tables are up to date.", st.success),
    "failed": ("Failed: ask whoever runs Airflow to look at this run.", st.error),
}
# What a running DAG is doing, in plain words (Publish waits for them).
BUSY_WORDS = {
    "dq_ingest_validate": "a new data file is being loaded",
    "dq_apply_corrections": "an uploaded correction file is being applied",
    "dq_publish": "a publish is already running",
}


def busy_with() -> list[str]:
    """What Airflow is running now that Publish must wait for."""

    try:
        return [BUSY_WORDS[d] for d in airflow_trigger.active_runs(list(BUSY_WORDS))]
    except airflow_trigger.AirflowError:
        return []


def publish_with_airflow(ctx: Context) -> None:
    """Trigger dq_publish for the run (merge, metrics, metadata, gold)."""

    try:
        dag_run = airflow_trigger.trigger(PUBLISH_DAG, {
            "run": ctx.layout.run_id, "user": ctx.user,
        })
    except airflow_trigger.AirflowError as error:
        st.error(f"Publishing could not be handed to Airflow: {error}")
        return
    st.session_state["publish_runs"] = [
        {"run_id": ctx.layout.run_id, "dag_run": dag_run,
         "file": f"Publish of {run_label(ctx.layout.run_id)}"},
        *st.session_state.get("publish_runs", []),
    ][:5]
    st.rerun()


def publish(ctx: Context) -> None:
    """Merge into the clean data and refresh the metric tables (no Airflow)."""

    with st.status("Publishing...", expanded=True) as progress:
        st.write("Adding the fixed records to the clean data...")
        summary = merge_run(ctx.layout, ctx.registry, published_by=ctx.user)
        st.write("Updating the quality figures for Power BI...")
        refresh_metrics()
        progress.update(label="Published", state="complete")

    st.session_state["last_publish"] = summary
    st.rerun()


def publish_section(ctx: Context, fixed: int) -> None:
    st.markdown("#### Publish to clean data")
    st.caption("Fixed records join the clean data (and the Power BI figures) "
               "only when they are published. Publish when you are done, e.g. "
               "once at the end of the day.")

    last = read_publish(ctx.layout)
    published = last["counts"]["resolved"] if last else 0
    waiting = fixed - published

    left, right = st.columns(2)
    left.metric("Fixed, waiting to be published", f"{max(waiting, 0):,}")
    if last:
        by = f" by {last['published_by']}" if last.get("published_by") else ""
        right.metric("Records in the clean data", f"{last['counts']['final_rows']:,}")
        st.caption(f"Last published {when(last['merged_at'])}{by}.")
    else:
        right.metric("Records in the clean data", "not published yet")

    done = st.session_state.pop("last_publish", None)
    if done:
        st.success(f"Published: the clean data now has "
                   f"{done['counts']['final_rows']:,} records "
                   f"({done['counts']['resolved']:,} of them fixed here).")

    label = ("Publish to clean data" if waiting > 0 or not last
             else "Publish again (nothing new)")
    if not airflow_trigger.enabled():
        if st.button(label, type="primary" if waiting > 0 else "secondary"):
            publish(ctx)
        return

    show_dag_runs(PUBLISH_DAG, [r for r in st.session_state.get("publish_runs", [])
                                if r["run_id"] == ctx.layout.run_id],
                  PUBLISH_STATES, "Publishing", "refresh_publish")
    busy = busy_with()
    if busy:
        st.info(f"Publish is available again when this is done: {'; '.join(busy)}.")
    if st.button(label, type="primary" if waiting > 0 else "secondary",
                 disabled=bool(busy)):
        publish_with_airflow(ctx)


def page_progress(ctx: Context) -> None:
    st.title("Progress")
    layout = ctx.layout
    status = read_status(layout, ledger_stamp(layout))
    counts = status["status"].value_counts()

    total = len(status)
    fixed = int(counts.get("RESOLVED", 0))
    removed = int(counts.get("DISCARDED", 0))
    expert = int(counts.get("MANUAL_REVIEW_REQUIRED", 0))
    waiting = int(counts.get("OPEN", 0))

    cells = st.columns(4)
    cells[0].metric("Waiting to be fixed", f"{waiting:,}")
    cells[1].metric("Fixed", f"{fixed:,}")
    cells[2].metric("Removed", f"{removed:,}")
    cells[3].metric("Need an expert", f"{expert:,}")
    if total:
        st.progress((fixed + removed) / total,
                    text=f"{(fixed + removed) / total:.0%} of the {total:,} records "
                    "that were put on hold are dealt with")

    with st.container(border=True):
        publish_section(ctx, fixed)

    round_number = current_round(status)
    if round_number is not None:
        errors, _ = read_round(
            layout, round_number, str(ctx.args.rules), ledger_stamp(layout),
        )
        blocking = errors[errors["blocking"]]
        remaining = (
            blocking.groupby("group")["record_id"].nunique()
            .sort_values(ascending=False).rename("Records")
        )
        st.markdown("#### What is still to do")
        st.bar_chart(remaining, horizontal=True)

    log = layout.load_log(latest=200).iloc[::-1]
    if len(log):
        st.markdown("#### Latest changes")
        st.dataframe(pd.DataFrame({
            "When": [when(t) for t in log["corrected_at"]],
            "Who": log["user"].to_numpy(),
            "Record": log["record_id"].to_numpy(),
            "Field": [column_label(c) if pd.notna(c) else "(whole record)"
                      for c in log["column"]],
            "Before": log["old_value"].to_numpy(),
            "After": [
                "removed" if action == "DISCARD"
                else "automatic changes accepted" if action == "ACCEPT"
                else value
                for action, value in zip(log["action"], log["new_value"])
            ],
            "Note": log["reason"].to_numpy(),
        }), hide_index=True)


# What can be written in the file, shown on the Upload page.
FILE_ACTIONS = pd.DataFrame([
    ["Correct a field", "the right value", "leave empty (or FIX)"],
    ["Empty a field on purpose", NULL_MARKER, "leave empty (or FIX)"],
    ["Remove the whole record", "leave empty", "DISCARD"],
    ["Accept the automatic changes (problem 'Many values changed "
     "automatically')", "leave empty", "ACCEPT"],
    ["Leave it for later", "leave empty", "leave empty"],
], columns=["To...", "new_value", "action"])


def what_happens(row: pd.Series) -> str:
    if row["action"] == "DISCARD":
        return "Remove the whole record"
    if row["action"] == "ACCEPT":
        return "Accept the automatic changes"
    field = column_label(row["column"])
    if row["new_value"] == NULL_MARKER:
        return f"Empty {lower_label(row['column'])}"
    before = row.get("old_value")
    before = "(empty)" if before is None or pd.isna(before) else before
    return f"{field}: {before} -> {row['new_value']}"


def page_upload(ctx: Context) -> None:
    st.title("Upload a file")
    st.caption("To correct many records at once, e.g. in Excel. Three steps:")
    layout = ctx.layout
    show_airflow_runs(layout)
    status = read_status(layout, ledger_stamp(layout))
    round_number = current_round(status)
    if round_number is None:
        st.success("Nothing is waiting to be fixed.")
        return

    errors, _ = read_round(layout, round_number, str(ctx.args.rules), ledger_stamp(layout))

    # ---- 1. Download ----
    with st.container(border=True):
        st.markdown("#### 1. Download the list of problems")
        groups = errors.groupby("group")["record_id"].nunique().sort_values(ascending=False)
        chosen = st.multiselect(
            "Only these problems (leave empty for all)", list(groups.index),
            format_func=lambda g: f"{g} ({groups[g]:,})",
        )
        selected = errors[errors["group"].isin(chosen)] if chosen else errors
        template = build_template(selected[[c for c in selected.columns
                                            if c not in ("group", "blocking")]])
        st.download_button(
            f"Download {len(template):,} lines (CSV, opens in Excel)",
            template.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"problems_{layout.run_id}_round_{round_number}.csv",
            mime="text/csv",
        )
        st.caption("One line per field to check: which record, which field, "
                   "its current value and what is wrong with it.")

    # ---- 2. Fill in ----
    with st.container(border=True):
        st.markdown("#### 2. Fill it in")
        st.markdown("Only fill in the columns **new_value**, **action** and "
                    "**reason**. Do not change the other columns.")
        st.dataframe(FILE_ACTIONS, hide_index=True)
        st.caption("action accepts only FIX, DISCARD or ACCEPT (any capitals). "
                   "reason is optional and saved with the change. Lines you do "
                   "not fill in are ignored.")

    # ---- 3. Upload and check ----
    with st.container(border=True):
        st.markdown("#### 3. Upload and check")
        uploaded = st.file_uploader("Your filled-in file (CSV)", type="csv")
        if uploaded is None:
            return

        lines = read_correction_file(uploaded)
        missing = {"record_id", "column", "new_value"} - set(lines.columns)
        if missing:
            st.error("This is not a filled-in problem list: the columns "
                     f"{', '.join(sorted(missing))} are missing.")
            return

        check = import_corrections(ctx.layout, ctx.registry, lines, ctx.user,
                                   dry_run=True)
        accepted, refused = check.accepted, check.refused
        removals = int(accepted["action"].eq("DISCARD").sum())
        approvals = int(accepted["action"].eq("ACCEPT").sum())
        ignored = len(lines) - len(accepted) - len(refused)

        cells = st.columns(5)
        cells[0].metric("Corrections", f"{len(accepted) - removals - approvals:,}")
        cells[1].metric("Accepted as they are", f"{approvals:,}")
        cells[2].metric("Records to remove", f"{removals:,}")
        cells[3].metric("Not filled in (ignored)", f"{ignored:,}")
        cells[4].metric("Cannot be used", f"{len(refused):,}")

        if len(refused):
            st.error("These lines cannot be used and will be skipped. Fix them "
                     "in the file and upload it again, or send the rest.")
            st.dataframe(pd.DataFrame({
                "Record": refused["record_id"].to_numpy(),
                "Field": [column_label(c) if c else "" for c in refused["column"]],
                "new_value": refused["new_value"].to_numpy(),
                "action": refused["action"].to_numpy(),
                "Why": [plain_refusal(r) for r in refused["refused"]],
            }), hide_index=True)

        if not len(accepted):
            st.warning("Nothing to send: no line has a usable change.")
            return

        st.markdown("**What will happen**")
        st.dataframe(pd.DataFrame({
            "Record": accepted["record_id"].to_numpy(),
            "Change": [what_happens(row) for _, row in accepted.iterrows()],
            "Reason": accepted["reason"].to_numpy(),
        }), hide_index=True)

        if not airflow_trigger.enabled():
            if st.button(f"Send {len(accepted):,} changes", type="primary"):
                send(ctx, lines, clear_changes=False)
            return

        st.caption("Sending hands the file to Airflow, which imports it, checks "
                   "the changed records against all the data and publishes the "
                   "result. You can follow it below.")
        if st.button(f"Send {len(accepted):,} changes", type="primary"):
            send_to_airflow(ctx, uploaded.getvalue())


# ============================================================
# Uploaded files through Airflow (dq_apply_corrections)
# ============================================================

CORRECTIONS_DAG = "dq_apply_corrections"
CORRECTIONS_FOLDER = Path("data") / "corrections"


def save_upload(ctx: Context, content: bytes) -> Path:
    """Keep the uploaded file where Airflow reads it; path relative to ROOT."""

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    who = re.sub(r"[^A-Za-z0-9_-]+", "_", ctx.user).strip("_") or "user"
    relative = CORRECTIONS_FOLDER / ctx.layout.run_id / f"{stamp}_{who}.csv"
    (ROOT / relative).parent.mkdir(parents=True, exist_ok=True)
    (ROOT / relative).write_bytes(content)
    return relative


def send_to_airflow(ctx: Context, content: bytes) -> None:
    """Save the file and trigger dq_apply_corrections with it."""

    relative = save_upload(ctx, content)
    try:
        dag_run = airflow_trigger.trigger(CORRECTIONS_DAG, {
            "run": ctx.layout.run_id,
            "file": relative.as_posix(),
            "user": ctx.user,
        })
    except airflow_trigger.AirflowError as error:
        st.error(f"The file could not be handed to Airflow: {error}")
        return
    st.session_state["airflow_runs"] = [
        {"run_id": ctx.layout.run_id, "dag_run": dag_run, "file": relative.as_posix()},
        *st.session_state.get("airflow_runs", []),
    ][:5]
    st.rerun()


AIRFLOW_STATES = {
    "queued": ("Waiting to start", st.info),
    "running": ("Importing and checking the changes...", st.info),
    "success": ("Done: the changes are imported, checked and published.", st.success),
    "failed": ("Failed: ask whoever runs Airflow to look at this run.", st.error),
}


def show_dag_runs(dag_id: str, runs: list[dict], states: dict, title: str,
                  key: str) -> None:
    """Runs of a DAG started from this session, with their state."""

    if not runs:
        return
    with st.container(border=True):
        st.markdown(f"#### {title}")
        finished = True
        for sent in runs:
            try:
                state = airflow_trigger.run_state(dag_id, sent["dag_run"])
            except airflow_trigger.AirflowError as error:
                state, text, show = "unknown", f"State unknown: {error}", st.warning
            else:
                text, show = states.get(state, (state, st.info))
            finished = finished and state in ("success", "failed")
            link = airflow_trigger.run_link(dag_id, sent["dag_run"])
            show(f"**{Path(sent['file']).name}**: {text}"
                 + (f" ([open in Airflow]({link}))" if link else ""))
        if st.button("Refresh", disabled=finished, key=key):
            st.cache_data.clear()
            st.rerun()


def show_airflow_runs(layout: Layout) -> None:
    """Uploaded correction files sent to Airflow from this session."""

    show_dag_runs(CORRECTIONS_DAG, [r for r in st.session_state.get("airflow_runs", [])
                                    if r["run_id"] == layout.run_id],
                  AIRFLOW_STATES, "Files sent to Airflow", "refresh_corrections")


# ============================================================
# Load new data through Airflow (dq_ingest_validate)
# ============================================================

INGEST_DAG = "dq_ingest_validate"
UPLOADS_FOLDER = Path("data") / "raw" / "uploads"
INGEST_STATES = {
    "queued": ("Waiting to start", st.info),
    "running": ("Loading and checking the data...", st.info),
    "success": ("Done: loaded, checked and published. Records with problems "
                "are waiting in Fix records (pick the new batch in Settings).",
                st.success),
    "failed": ("Failed: ask whoever runs Airflow to look at this run.", st.error),
}


def file_name_part(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", text).strip("_")


def loaded_as(dataset: str, sha256: str) -> str | None:
    """The ingestion that already holds a file with this content."""

    with db.transaction() as conn:
        row = conn.execute(
            "SELECT ingestion_id FROM bronze.ingestion "
            "WHERE dataset = %s AND sha256 = %s ORDER BY ingestion_id LIMIT 1",
            (dataset, sha256),
        ).fetchone()
    return row[0] if row else None


def read_header(content: bytes) -> list[str]:
    first = content[:1 << 16].decode("utf-8-sig", errors="replace").splitlines()
    return next(csv.reader(first[:1]), []) if first else []


def send_new_data(dataset: str, user: str, name: str, content: bytes) -> None:
    """Save the file where Airflow reads it and trigger dq_ingest_validate."""

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    relative = (UPLOADS_FOLDER / dataset /
                f"{stamp}_{file_name_part(user) or 'user'}_"
                f"{file_name_part(Path(name).stem) or 'data'}.csv")
    (ROOT / relative).parent.mkdir(parents=True, exist_ok=True)
    (ROOT / relative).write_bytes(content)
    try:
        dag_run = airflow_trigger.trigger(INGEST_DAG, {"input": relative.as_posix()})
    except airflow_trigger.AirflowError as error:
        st.error(f"The file could not be handed to Airflow: {error}")
        return
    st.session_state["ingest_runs"] = [
        {"dag_run": dag_run, "file": name},
        *st.session_state.get("ingest_runs", []),
    ][:5]
    st.rerun()


def show_ingest_runs() -> None:
    """New data files sent to Airflow from this session."""

    show_dag_runs(INGEST_DAG, st.session_state.get("ingest_runs", []),
                  INGEST_STATES, "Files being loaded", "refresh_ingest")


def page_load(registry: Registry, user: str) -> None:
    st.title("Load new data")
    st.caption("Upload a new data file (CSV). It is loaded, checked and the "
               "valid records are published; records with problems wait in "
               "Fix records. A file that was loaded before is not loaded again; "
               "records already in the data are only updated when their values "
               "changed.")
    show_ingest_runs()
    if not airflow_trigger.enabled():
        st.info("Loading files from here needs Airflow (AIRFLOW_API_URL). "
                "Without it: python dq.py pipeline --input <file>.")
        return

    dataset = registry.dataset.get("name", "dataset")
    uploaded = st.file_uploader("Data file", type=["csv"], key="new_data")
    if uploaded is None:
        return
    content = uploaded.getvalue()

    header = read_header(content)
    expected = list(registry.columns)
    key = list(registry.dataset.get("primary_key") or [])
    missing = [c for c in expected if c not in header]
    extra = [c for c in header if c not in expected]
    previous = loaded_as(dataset, hashlib.sha256(content).hexdigest())

    sizes = st.columns(3)
    sizes[0].metric("Size", f"{len(content) / 1e6:,.1f} MB")
    sizes[1].metric("Columns", len(header))
    lines = max(content.count(b"\n") - 1, 0)
    sizes[2].metric("Lines", f"{lines:,}")
    try:
        st.dataframe(pd.read_csv(io.BytesIO(content), nrows=5, dtype=str,
                                 keep_default_na=False, encoding="utf-8-sig"),
                     hide_index=True)
    except (ValueError, UnicodeDecodeError) as error:
        st.error(f"The file cannot be read as CSV: {error}")
        return

    problems = []
    if previous:
        problems.append(f"This exact file was already loaded ({run_label(previous)}).")
    if missing_key := [c for c in key if c not in header]:
        problems.append(f"The file has no {', '.join(missing_key)} column.")
    if problems:
        for problem in problems:
            st.error(problem)
        return
    if missing:
        st.warning(f"Columns missing from the file (their checks will fail): "
                   f"{', '.join(missing)}")
    if extra:
        st.warning(f"Columns the data does not know (loaded as they are): "
                   f"{', '.join(extra)}")

    if st.button(f"Load {uploaded.name}", type="primary"):
        send_new_data(dataset, user, uploaded.name, content)


# ============================================================
# Main
# ============================================================

def run_label(run_id: str) -> str:
    """'ING_20261011_001' -> 'Loaded 11 Oct 2026 (no. 1)'."""

    match = re.fullmatch(r"ING_(\d{4})(\d{2})(\d{2})_(\d+)", run_id)
    if not match:
        return run_id
    year, month, day, number = match.groups()
    loaded = date(int(year), int(month), int(day))
    return f"Loaded {loaded:%d %b %Y} (no. {int(number)})"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DQ correction UI.")
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    args, _ = parser.parse_known_args(sys.argv[1:])
    return args


def main() -> None:
    args = parse_args()
    st.set_page_config(page_title="Data corrections", layout="wide")
    registry = get_registry(str(args.rules))
    dataset = registry.dataset.get("name", "dataset")

    runs = list_runs(dataset)

    with st.sidebar:
        st.title("Data corrections")
        user = st.text_input("Your name", value=getpass.getuser()).strip()
        if runs:
            with st.expander("Settings"):
                run_id = st.selectbox("Data batch", runs[::-1], format_func=run_label)
                max_attempts = st.number_input(
                    "Send an expert a record after this many failed checks",
                    1, 20, DEFAULT_MAX_ATTEMPTS,
                )

    pages = {}
    if runs:
        layout = Layout(dataset, run_id)
        pending = len(my_changes(layout))
        pages = {
            "Fix records": page_fix,
            f"My changes ({pending})": page_changes,
            "Progress": page_progress,
            "Upload a file": page_upload,
        }
    pages["Load new data"] = None
    with st.sidebar:
        page = st.radio("Go to", list(pages), label_visibility="collapsed")

    if runs:
        show_last_send(layout)
    if not user:
        st.warning("Enter your name in the sidebar first; it is saved with "
                   "every change.")
        return
    if pages[page] is None:
        page_load(registry, user)
        return

    ctx = Context(args, registry, get_wording(str(args.rules)), layout,
                  user, int(max_attempts))
    pages[page](ctx)


if __name__ == "__main__":
    main()
