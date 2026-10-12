"""Plain-language help for people correcting quarantined records.

Used by the correction UI so that a business user never has to read rule
ids, error codes or regular expressions:

    column_label      'customer_first_name' -> 'Customer first name'
    problem_text      one error line -> 'Customer first name is missing'
    problem_group     error line -> the group it is listed under
                      ('Missing customer first name')
    field_input       how a field should be entered (list, date, number,
                      text with an example) and its accepted range
    check_value       is a typed value acceptable for its field?
    Suggestions       values for a field taken from other, valid records
                      of the same customer, account, branch, ... (the
                      schema's entities and dependencies)
    changes_to_lines  the user's changes -> correction lines for
                      quarantine.import_corrections

Everything here is derived from the schema registry and the rules, so it
follows the dataset without code changes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pandas as pd

from quarantine import NULL_MARKER
from registry import Registry, Rule
from values import resolve_bound


# Words that read better in capitals.
UPPER_WORDS = {"id": "ID", "egp": "EGP", "iso": "ISO"}

TYPE_WORDS = {
    "date": "date (YYYY-MM-DD)",
    "time": "time (HH:MM:SS)",
    "year_month": "month (YYYY-MM)",
    "integer": "whole number",
    "decimal": "number",
    "boolean": "yes/no value",
}

LINE_COLUMNS = ["record_id", "column", "new_value", "action", "reason"]

# Text formats of types without their own input (pattern, example).
FORMAT_PATTERNS = {
    "time": (r"([01]\d|2[0-3]):[0-5]\d:[0-5]\d", "14:30:00"),
    "year_month": (r"\d{4}-(0[1-9]|1[0-2])", "2024-05"),
}

# What each automatic repair did, in everyday words.
REPAIR_WORDS = {
    "trim": "Removed extra spaces",
    "set_null": "Placeholder emptied",
    "canonicalize": "Standard spelling",
    "parse_number": "Number cleaned up",
    "parse_date": "Date put in YYYY-MM-DD",
    "parse_time": "Time put in 24-hour format",
    "reformat_id": "ID put in the standard format",
    "replace": "Known mistake corrected",
    "derive": "Recalculated from other fields",
    "swap_day_month": "Day and month swapped",
}


# ============================================================
# Wording
# ============================================================

def column_label(column: str | None) -> str:
    if not column:
        return "Record"
    words = [UPPER_WORDS.get(w, w) for w in column.split("_")]
    text = " ".join(words)
    return text[0].upper() + text[1:]


def lower_label(column: str | None) -> str:
    """Label inside a sentence: 'customer ID', 'email'."""

    label = column_label(column)
    return label[0].lower() + label[1:]


def _quoted(value: Any) -> str:
    return "" if value is None or pd.isna(value) else f' "{value}"'


def _bounds_text(spec: dict[str, Any]) -> str:
    low, high = spec.get("min"), spec.get("max")
    if low is not None and high is not None:
        return f"between {low} and {high}"
    if low is not None:
        return f"above {low}" if spec.get("exclusive_min") else f"at least {low}"
    if high is not None:
        return f"below {high}" if spec.get("exclusive_max") else f"at most {high}"
    return ""


class Wording:
    """Plain-language text for the errors of one registry."""

    def __init__(self, registry: Registry) -> None:
        self.registry = registry
        self.rules: dict[str, Rule] = {r.id: r for r in registry.all_rules()}
        self.entities = registry.schema.get("entities", {}) or {}
        self.key_entity = {
            spec["key"]: name for name, spec in self.entities.items()
        }

    def spec(self, column: str | None) -> dict[str, Any]:
        return self.registry.columns.get(column or "", {})

    def _explanation(self, rule: Rule | None) -> str:
        """The rule's own name, for rules written in the rules file."""

        if rule is None or rule.origin == "schema" or not rule.name:
            return ""
        return rule.name

    def problem_text(self, error: pd.Series) -> str:
        """One sentence for one error line."""

        column = error.get("column")
        if pd.isna(column):
            column = None
        label = column_label(column)
        value = error.get("value")
        rule = self.rules.get(error.get("rule_id"))
        category = error.get("error_category")
        spec = self.spec(column)

        if category == "null":
            text = f"{label} is missing"
        elif category == "duplicate":
            text = (
                "This record is an exact copy of another record"
                if rule is not None and rule.type == "duplicate_rows"
                else f"Another record has the same {lower_label(column)}{_quoted(value)}"
            )
        elif category == "range_violation":
            text = (f"{label}{_quoted(value)} must be {_bounds_text(spec)}"
                    if _bounds_text(spec) else f"{label}{_quoted(value)} is out of range")
        elif category == "format_violation":
            text = f"{label}{_quoted(value)} is not written correctly"
        elif category == "domain_violation":
            text = f"{label}{_quoted(value)} is not one of the accepted values"
        elif category == "type_mismatch":
            kind = TYPE_WORDS.get(spec.get("type", ""), "value")
            text = f"{label}{_quoted(value)} is not a valid {kind}"
        elif category == "referential_integrity":
            text = f"{label}{_quoted(value)} is not a known {lower_label(column)}"
        elif category == "consistency":
            text = (f"Other records with the same {lower_label(column)}"
                    f"{_quoted(value)} show different details")
        elif category == "auto_fix_review":
            count = str(value).split(" ", 1)[0] if value is not None and not pd.isna(value) else "Several"
            return (f"{count} values were changed automatically; please check "
                    "that the changes are right")
        else:
            text = rule.name if rule is not None and rule.name else str(error.get("message"))
            return text

        explanation = self._explanation(rule)
        return f"{text} ({explanation})" if explanation else text

    def group_title(self, category: str, column: str | None) -> str:
        """Title of a group of errors, as offered in 'What to fix'."""

        label = column_label(column)
        lower = lower_label(column)
        if category == "null":
            return f"Missing {lower}"
        if category == "duplicate":
            return f"Duplicate {lower}" if column else "Duplicate records"
        if category == "range_violation":
            return f"{label} out of range"
        if category == "format_violation":
            return f"{label} written incorrectly"
        if category == "domain_violation":
            return f"{label} not an accepted value"
        if category == "type_mismatch":
            return f"{label} not a valid {TYPE_WORDS.get(self.spec(column).get('type', ''), 'value')}"
        if category == "referential_integrity":
            return f"Unknown {lower}"
        if category == "auto_fix_review":
            return "Many values changed automatically"
        if category == "consistency":
            entity = self.key_entity.get(column or "")
            return (f"{column_label(entity)} details differ between records"
                    if entity else f"Details differ for the same {lower}")
        return f"Other problems with {lower}" if column else "Other problems"

    def add_groups(self, errors: pd.DataFrame) -> pd.DataFrame:
        """errors plus a 'group' column with the plain group title."""

        keys = errors[["error_category", "column"]].drop_duplicates()
        keys["group"] = [
            self.group_title(category, None if pd.isna(column) else column)
            for category, column in zip(keys["error_category"], keys["column"])
        ]
        return errors.merge(keys, on=["error_category", "column"], how="left")


# ============================================================
# Entering values
# ============================================================

@dataclass
class FieldInput:
    """How a value for a column is entered and checked."""

    column: str
    label: str
    help: str
    kind: str                      # choice | date | number | text
    options: list[str] = field(default_factory=list)
    low: Any = None
    high: Any = None
    exclusive_low: bool = False
    exclusive_high: bool = False
    bounds: str = ""
    integer: bool = False
    pattern: str | None = None
    example: str | None = None
    can_be_empty: bool = False


def field_input(registry: Registry, column: str, example: str | None = None) -> FieldInput:
    spec = registry.columns.get(column, {})
    column_type = spec.get("type", "string")
    help_text = spec.get("description", "")
    bounds = _bounds_text(spec)
    if bounds:
        help_text += f" Must be {bounds}."

    base = dict(
        column=column,
        label=column_label(column),
        help=help_text.strip(),
        can_be_empty=spec.get("nullable", True) is not False,
        example=example,
    )

    if "allowed_values" in spec:
        return FieldInput(
            kind="choice",
            options=[str(v) for v in spec["allowed_values"]],
            **base,
        )
    if column_type == "date":
        return FieldInput(
            kind="date",
            low=_date_bound(spec.get("min"), column_type) or date(1900, 1, 1),
            high=_date_bound(spec.get("max"), column_type) or date(2100, 12, 31),
            **base,
        )
    if column_type in ("integer", "decimal"):
        return FieldInput(
            kind="number",
            low=None if spec.get("min") is None else float(spec["min"]),
            high=None if spec.get("max") is None else float(spec["max"]),
            exclusive_low=bool(spec.get("exclusive_min")),
            exclusive_high=bool(spec.get("exclusive_max")),
            bounds=bounds,
            integer=column_type == "integer",
            **base,
        )
    if column_type in FORMAT_PATTERNS and "pattern" not in spec:
        pattern, example = FORMAT_PATTERNS[column_type]
        return FieldInput(kind="text", pattern=pattern,
                          **{**base, "example": base["example"] or example})
    return FieldInput(kind="text", pattern=spec.get("pattern"), **base)


def _date_bound(value: Any, column_type: str) -> date | None:
    if value is None:
        return None
    return resolve_bound(value, column_type).date()


def check_value(spec: FieldInput, text: str) -> str:
    """'' when the value is acceptable, otherwise what is wrong with it."""

    if text == "":
        return "" if spec.can_be_empty else f"{spec.label} cannot be empty."
    if spec.kind == "choice" and text not in spec.options:
        return f"Choose one of the listed values for {lower_label(spec.column)}."
    if spec.kind == "number":
        kind = "a whole number" if spec.integer else "a number"
        need = f"Enter {kind}" + (f" {spec.bounds}" if spec.bounds else "") + "."
        try:
            number = float(text)
        except ValueError:
            return need
        if spec.integer and not number.is_integer():
            return need
        if spec.low is not None and (
            number < spec.low or (spec.exclusive_low and number == spec.low)
        ):
            return need
        if spec.high is not None and (
            number > spec.high or (spec.exclusive_high and number == spec.high)
        ):
            return need
    if spec.pattern and not re.fullmatch(spec.pattern, text):
        hint = f" For example: {spec.example}" if spec.example else ""
        return f"{spec.label} is not written the expected way.{hint}"
    return ""


# ============================================================
# Suggestions from other records
# ============================================================

@dataclass
class Suggestion:
    value: str
    source_column: str
    source_value: str
    support: int
    total: int

    def explain(self) -> str:
        return explain_suggestion(
            self.source_column, self.source_value, self.support, self.total,
        )


def explain_suggestion(source_column: str, source_value: Any, support: int, total: int) -> str:
    return (f"Found on {support} of {total} other records with "
            f"{lower_label(source_column)} {source_value}")


def dependency_pairs(registry: Registry) -> list[tuple[str, str]]:
    """(determinant, dependent) pairs declared in the schema."""

    schema = registry.schema
    pairs = []
    for entity in (schema.get("entities") or {}).values():
        dependents = list(entity.get("attributes", []))
        dependents += list((entity.get("references") or {}).values())
        pairs += [(entity["key"], column) for column in dependents]
    for dependency in (schema.get("dependencies") or {}).values():
        pairs += [(dependency["determinant"], c) for c in dependency["dependents"]]
    return [
        (key, column) for key, column in pairs
        if key in registry.columns and column in registry.columns
    ]


class Suggestions:
    """Most common value of a column per key, from accepted records."""

    def __init__(self, registry: Registry, valid: pd.DataFrame) -> None:
        self.tables: dict[tuple[str, str], pd.DataFrame] = {}
        self.examples: dict[str, str] = {}

        for key, column in dependency_pairs(registry):
            if key not in valid.columns or column not in valid.columns:
                continue
            pairs = valid[[key, column]].dropna()
            counts = pairs.value_counts().rename("support").reset_index()
            best = counts.drop_duplicates(key).set_index(key)
            best["total"] = counts.groupby(key)["support"].sum()
            self.tables[(key, column)] = best.rename(columns={column: "value"})

        for column in valid.columns:
            values = valid[column].dropna()
            if len(values):
                self.examples[column] = str(values.iloc[0])

    @staticmethod
    def columns_needed(registry: Registry) -> list[str]:
        needed = {c for pair in dependency_pairs(registry) for c in pair}
        needed |= {
            column for column, spec in registry.columns.items()
            if "pattern" in spec
        }
        return sorted(needed)

    def for_column(self, values: dict[str, Any], column: str) -> Suggestion | None:
        """Best suggestion for one column given the record's other values."""

        best: Suggestion | None = None
        for (key, target), table in self.tables.items():
            if target != column:
                continue
            key_value = values.get(key)
            if key_value is None or pd.isna(key_value) or key_value not in table.index:
                continue
            row = table.loc[key_value]
            candidate = Suggestion(
                value=str(row["value"]),
                source_column=key,
                source_value=str(key_value),
                support=int(row["support"]),
                total=int(row["total"]),
            )
            if best is None or candidate.support > best.support:
                best = candidate
        return best

    def for_records(self, records: pd.DataFrame, needs: pd.DataFrame) -> pd.DataFrame:
        """Suggestions for many records at once.

        needs: one boolean column per field to fill, same index as records.
        Returns one row per record and field found: record index, column,
        value, source_column, source_value, support, total.
        """

        values = records.copy()
        found: list[pd.DataFrame] = []
        done = pd.DataFrame(False, index=needs.index, columns=needs.columns)

        for _ in range(2):
            for column in needs.columns:
                pending = needs[column] & ~done[column]
                if not pending.any():
                    continue
                best = pd.DataFrame(index=pending[pending].index)
                best["support"] = 0
                for (key, target), table in self.tables.items():
                    if target != column or key not in values.columns:
                        continue
                    keys = values.loc[best.index, key]
                    support = keys.map(table["support"]).fillna(0).astype(int)
                    better = support > best["support"]
                    if not better.any():
                        continue
                    index = better[better].index
                    best.loc[index, "support"] = support[index]
                    best.loc[index, "value"] = keys[index].map(table["value"])
                    best.loc[index, "total"] = keys[index].map(table["total"])
                    best.loc[index, "source_column"] = key
                    best.loc[index, "source_value"] = keys[index]

                best = best[best["support"] > 0]
                if not len(best):
                    continue
                done.loc[best.index, column] = True
                values.loc[best.index, column] = best["value"].astype("string")
                found.append(best.assign(column=column))

        if not found:
            return pd.DataFrame(columns=[
                "column", "value", "source_column", "source_value", "support", "total",
            ])
        result = pd.concat(found)
        result["total"] = result["total"].astype(int)
        return result

    def for_record(
        self, record: dict[str, Any], columns: list[str],
    ) -> dict[str, Suggestion]:
        """Suggestions for several columns of one record.

        A suggested key (e.g. customer_id found through account_id) is used
        to find the others (the customer's name), so two passes.
        """

        values = dict(record)
        found: dict[str, Suggestion] = {}
        for _ in range(2):
            for column in columns:
                if column in found:
                    continue
                suggestion = self.for_column(values, column)
                if suggestion is not None:
                    found[column] = suggestion
                    values[column] = suggestion.value
        return found


# ============================================================
# Changes -> correction lines
# ============================================================

@dataclass
class Change:
    """What the user decided for one record."""

    record_id: str
    fixes: dict[str, tuple[str, str]] = field(default_factory=dict)  # column -> (value, reason)
    discard_reason: str | None = None
    # Confirms the record's automatic changes (with or without fixes).
    accept_reason: str | None = None

    @property
    def discard(self) -> bool:
        return self.discard_reason is not None

    @property
    def accept(self) -> bool:
        return self.accept_reason is not None

    def summary(self) -> str:
        if self.discard:
            return f"Remove the record ({self.discard_reason})"
        parts = ["Accept the automatic changes"] if self.accept else []
        parts += [
            f"{column_label(column)} -> " + ("(empty)" if value == "" else value)
            for column, (value, _) in self.fixes.items()
        ]
        return "; ".join(parts)


def changes_to_lines(changes: list[Change]) -> pd.DataFrame:
    """The same lines a filled-in correction template would contain."""

    lines = []
    for change in changes:
        if change.discard:
            lines.append([change.record_id, "", "", "DISCARD", change.discard_reason])
            continue
        if change.accept:
            lines.append([change.record_id, "", "", "ACCEPT", change.accept_reason])
        for column, (value, reason) in change.fixes.items():
            lines.append([
                change.record_id, column,
                NULL_MARKER if value == "" else value, "FIX", reason,
            ])
    return pd.DataFrame(lines, columns=LINE_COLUMNS, dtype="string")
