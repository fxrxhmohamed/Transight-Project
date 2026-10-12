"""Value parsing and formatting shared by checks and remediation (Spark SQL).

The engine keeps every column as text (null for missing) so the original
spelling survives until a rule decides to change it. These helpers build
Spark SQL expressions (text) that turn that text into typed values for
comparisons, and typed values back into canonical text for output. Values
that do not parse become null (the session runs with ANSI mode off).

Expressions are built as SQL text, not as pyspark Column objects: a stage
of a few hundred expressions then reaches Spark in one call instead of
tens of thousands, which keeps planning in the sub-second range.

Date and time formats are written as in Python (strptime, e.g. %d/%m/%Y)
in the configuration and translated to Spark patterns here. Regular
expressions are Java regular expressions; the few Python-only spellings
((?P<name>...), \\g<1>) are translated.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from dateutil.relativedelta import relativedelta
from pyspark.sql import Column
from pyspark.sql import functions as F


ISO_DATE_FORMAT = "%Y-%m-%d"
ISO_TIME_FORMAT = "%H:%M:%S"

# A number inside text, e.g. 'EGP 12,284.15' or '3780.11 LE'.
NUMBER_IN_TEXT_PATTERN = r"(?U)[+-]?\d[\d,.\s]*\d|[+-]?\d"

# What a plain number looks like (what pandas.to_numeric accepts).
NUMBER_PATTERN = r"^\s*[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?\s*$"

# hh:mm, hh:mm:ss or hh.mm.ss, optionally on a 12-hour clock (07:19:20 PM)
TIME_TEXT_PATTERN = (
    r"^(\d{1,2})[:.](\d{2})(?:[:.](\d{2}))?(?:\s*([AaPp])\.?[Mm]\.?)?$"
)

# "today", "today-18y", "today+30d": a bound relative to the run date.
RELATIVE_DATE_PATTERN = r"^today(?:\s*([+-])\s*(\d+)\s*([ymd]))?$"

NUMERIC_TYPES = ("integer", "decimal")
TEMPORAL_TYPES = ("date", "time", "year_month")

CANONICAL_FORMATS = {
    "date": ISO_DATE_FORMAT,
    "time": ISO_TIME_FORMAT,
    "year_month": "%Y-%m",
}

# strptime directive -> (Spark pattern when parsing, when formatting).
# Parsing accepts one or two digits, as strptime does, unless two numeric
# directives touch (%Y%m%d), where fixed widths are needed.
_DIRECTIVES = {
    "Y": ("yyyy", "yyyy"),
    "y": ("yy", "yy"),
    "m": ("M", "MM"),
    "d": ("d", "dd"),
    "b": ("MMM", "MMM"),
    "B": ("MMMM", "MMMM"),
    "H": ("H", "HH"),
    "I": ("h", "hh"),
    "M": ("m", "mm"),
    "S": ("s", "ss"),
    "p": ("a", "a"),
}

STRIP_PATTERN = r"(?U)^\s+|\s+$"
SEPARATORS_PATTERN = r"(?U)[\s_\-]+"
SPACE_PATTERN = r"(?U)\s"
TRAILING_ZEROS_PATTERN = r"\.?0+$"

NULL = "CAST(NULL AS STRING)"
TRUE, FALSE = "true", "false"


# ============================================================
# SQL text
# ============================================================

def ident(name: str) -> str:
    """A column name as SQL, whatever characters it has."""

    return "`" + name.replace("`", "``") + "`"


def col(name: str) -> Column:
    """A column by name as a pyspark Column (outside the rule engine)."""

    return F.col(ident(name))


def lit(value: Any) -> str:
    """A Python value as a SQL literal."""

    if value is None:
        return NULL
    if isinstance(value, bool):
        return TRUE if value else FALSE
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"CAST({value!r} AS DOUBLE)"
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value:%Y-%m-%d %H:%M:%S}'"
    if isinstance(value, date):
        return f"DATE '{value:%Y-%m-%d}'"
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    text = text.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    return f"'{text}'"


def flag(condition: str) -> str:
    """A nullable condition as plain true/false (null -> false)."""

    return f"coalesce({condition}, false)"


def all_of(*conditions: str) -> str:
    return "(" + " AND ".join(conditions) + ")" if conditions else TRUE


def any_of(*conditions: str) -> str:
    return "(" + " OR ".join(conditions) + ")" if conditions else FALSE


def is_in(values: str, options: list[Any]) -> str:
    """values IN (...) (null for a null value)."""

    if not options:
        return f"CASE WHEN {values} IS NULL THEN NULL ELSE false END"
    return f"({values} IN ({', '.join(lit(str(o)) for o in options)}))"


def case(*branches: tuple[str, str], otherwise: str = NULL) -> str:
    if not branches:
        return otherwise
    body = " ".join(f"WHEN {c} THEN {v}" for c, v in branches)
    return f"CASE {body} ELSE {otherwise} END"


# ============================================================
# Formats and regular expressions
# ============================================================

def spark_pattern(fmt: str, parsing: bool = True) -> str:
    """strptime format -> Spark datetime pattern."""

    tokens = re.findall(r"%.|[^%]+", fmt)
    adjacent = any(
        a.startswith("%") and b.startswith("%") and a != "%%" and b != "%%"
        for a, b in zip(tokens, tokens[1:])
    )
    out = []
    for token in tokens:
        if token == "%%":
            out.append("%")
        elif token.startswith("%"):
            code = token[1]
            if code not in _DIRECTIVES:
                raise ValueError(f"Unsupported date/time directive {token} in {fmt!r}")
            loose, fixed = _DIRECTIVES[code]
            out.append(loose if parsing and not adjacent else fixed)
        else:
            # Letters are pattern characters in Spark: quote literal text.
            out.append("'" + token.replace("'", "''") + "'"
                       if re.search(r"[A-Za-z']", token) else token)
    return "".join(out)


def java_regex(pattern: str) -> str:
    """A Python regular expression in Java syntax."""

    pattern = re.sub(r"\(\?P<", "(?<", pattern)
    return re.sub(r"\(\?P=(\w+)\)", r"\\k<\1>", pattern)


def java_replacement(text: str) -> str:
    """A Python re.sub replacement in Java syntax (\\1, \\g<1> -> $1)."""

    out = []
    i = 0
    while i < len(text):
        char = text[i]
        if char == "\\" and i + 1 < len(text):
            nxt = text[i + 1]
            group = re.match(r"\\g<(\w+)>", text[i:])
            if group:
                name = group.group(1)
                out.append(f"${name}" if name.isdigit() else "${" + name + "}")
                i += len(group.group(0))
                continue
            if nxt.isdigit():
                out.append("$" + nxt)
            else:
                out.append({"n": "\n", "t": "\t", "\\": "\\\\"}.get(nxt, nxt))
            i += 2
            continue
        out.append("\\$" if char == "$" else char)
        i += 1
    return "".join(out)


def rlike(values: str, regex: str) -> str:
    return f"({values} RLIKE {lit(regex)})"


def full_match(values: str, pattern: str) -> str:
    """True when the whole value matches (re.fullmatch)."""

    return rlike(values, r"^(?:" + java_regex(pattern) + r")\z")


# ============================================================
# Text
# ============================================================

def strip(values: str) -> str:
    """str.strip(): surrounding whitespace, Unicode spaces included."""

    return f"regexp_replace({values}, {lit(STRIP_PATTERN)}, '')"


def normalize_key(values: str) -> str:
    """Fold case, spacing and separators: ' Internet_Banking' ->
    'internet banking'."""

    return f"regexp_replace(lower({strip(values)}), {lit(SEPARATORS_PATTERN)}, ' ')"


def normalize_one(value: Any) -> str:
    """normalize_key for a single value."""

    return re.sub(r"[\s_\-]+", " ", str(value).strip().casefold())


def lookup(mapping: dict[str, str], key: str) -> str:
    """mapping[key], null when the key is not in it."""

    if not mapping:
        return NULL
    pairs = ", ".join(f"{lit(k)}, {lit(v)}" for k, v in mapping.items())
    return f"element_at(map({pairs}), {key})"


def empty_to_null(values: str) -> str:
    return f"nullif({values}, '')"


# ============================================================
# Numbers
# ============================================================

def to_number(values: str) -> str:
    """Text -> double; null when it is not a plain finite number."""

    number = f"CAST(trim({values}) AS DOUBLE)"
    return (f"CASE WHEN {rlike(values, NUMBER_PATTERN)} "
            f"AND abs({number}) < CAST('Infinity' AS DOUBLE) THEN {number} END")


def format_number(number: str) -> str:
    """Plain decimal text without exponent or trailing zeros:
    52400.0 -> '52400', 12284.15 -> '12284.15'."""

    text = f"CAST(CAST({number} AS DECIMAL(38,12)) AS STRING)"
    return (f"CASE WHEN instr({text}, '.') > 0 "
            f"THEN regexp_replace({text}, {lit(TRAILING_ZEROS_PATTERN)}, '') ELSE {text} END")


def recover_numbers(values: str) -> str:
    """Read numbers written with currency text or other separators.

    '3780.11 LE' -> 3780.11, 'EGP 12,284.15' -> 12284.15,
    '1786,35' -> 1786.35. Unreadable values -> null.
    """

    number = empty_to_null(
        f"regexp_replace(regexp_extract({strip(values)}, "
        f"{lit('(' + NUMBER_IN_TEXT_PATTERN + ')')}, 1), {lit(SPACE_PATTERN)}, '')"
    )
    has_comma = f"(instr({number}, ',') > 0)"
    has_dot = f"(instr({number}, '.') > 0)"

    # '1786,35' or '1.234,56': the comma is the decimal mark.
    decimal_comma = any_of(
        all_of(has_comma, f"NOT {has_dot}", rlike(number, r"^[+-]?\d+,\d{1,2}$")),
        all_of(has_comma, has_dot, rlike(number, r",[^.]*$")),
    )
    cleaned = (
        f"CASE WHEN {decimal_comma} THEN replace(replace({number}, '.', ''), ',', '.') "
        f"ELSE replace({number}, ',', '') END"
    )
    return to_number(cleaned)


# ============================================================
# Dates and times
# ============================================================

def parse_temporal(values: str, formats: list[str], column_type: str = "date") -> str:
    """Parse with the first format that fits each value.

    Dates (and year_month) become DATE, times a TIMESTAMP on 1970-01-01.
    """

    parse = "to_timestamp" if column_type == "time" else "to_date"
    parts = [f"{parse}({values}, {lit(spark_pattern(f))})" for f in formats]
    return parts[0] if len(parts) == 1 else f"coalesce({', '.join(parts)})"


def format_temporal(typed: str, fmt: str) -> str:
    return f"date_format({typed}, {lit(spark_pattern(fmt, parsing=False))})"


def repair_times(values: str) -> str:
    """'12.30.15' -> '12:30:15', '9:05' -> '09:05:00',
    '07:19:20 PM' -> '19:19:20', '12:05:00 AM' -> '00:05:00'.

    Times outside 00:00:00-23:59:59 (or 1-12 o'clock with AM/PM) are not
    repaired (null).
    """

    text = strip(values)
    pattern = lit(TIME_TEXT_PATTERN)

    def group(n: int) -> str:
        return f"regexp_extract({text}, {pattern}, {n})"

    hour = f"CAST({group(1)} AS INT)"
    minute = f"CAST({group(2)} AS INT)"
    second = f"coalesce(CAST(nullif({group(3)}, '') AS INT), 0)"
    meridiem = f"lower({group(4)})"
    twelve_hour = f"({meridiem} != '')"

    valid_hour = any_of(all_of(f"NOT {twelve_hour}", f"{hour} <= 23"),
                        all_of(twelve_hour, f"{hour} BETWEEN 1 AND 12"))
    hour24 = (f"CASE WHEN {twelve_hour} THEN {hour} % 12 + "
              f"CASE WHEN {meridiem} = 'p' THEN 12 ELSE 0 END ELSE {hour} END")
    valid = all_of(f"({text} RLIKE {pattern})", valid_hour,
                   f"{minute} <= 59", f"{second} <= 59")
    return f"CASE WHEN {valid} THEN format_string('%02d:%02d:%02d', {hour24}, {minute}, {second}) END"


def resolve_bound(value: Any, column_type: str) -> Any:
    """Turn a schema min/max into a comparable value: a datetime for
    temporal columns ("today", "today-18y", "2022-01-01"), else a float."""

    if value is None:
        return None

    if column_type in TEMPORAL_TYPES:
        relative = re.match(RELATIVE_DATE_PATTERN, str(value).strip())
        if relative:
            bound = datetime.combine(date.today(), datetime.min.time())
            sign, amount, unit = relative.groups()
            if amount:
                unit = {"y": "years", "m": "months", "d": "days"}[unit]
                step = relativedelta(**{unit: int(amount)})
                bound = bound - step if sign == "-" else bound + step
            return bound
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        return datetime.fromisoformat(str(value))

    return float(value)


# ============================================================
# Typed view and canonical text
# ============================================================

def parse_values(values: str, spec: dict[str, Any]) -> str:
    """Typed view of a text column according to its schema entry.

    Invalid values become null. Dates and times are accepted in the source
    format and in the canonical (remediated) format.
    """

    column_type = spec.get("type", "string")

    if column_type in NUMERIC_TYPES:
        number = to_number(values)
        if column_type == "integer":
            return f"CASE WHEN {number} = floor({number}) THEN {number} END"
        return number

    if column_type == "boolean":
        true_values = [str(v) for v in spec.get("true_values", ["true", "1"])]
        false_values = [str(v) for v in spec.get("false_values", ["false", "0"])]
        return (f"CASE WHEN {flag(is_in(values, true_values))} THEN true "
                f"WHEN {flag(is_in(values, false_values))} THEN false END")

    if column_type in TEMPORAL_TYPES:
        canonical = CANONICAL_FORMATS[column_type]
        formats = [spec.get("source_format", canonical)]
        if canonical not in formats:
            formats.append(canonical)
        return parse_temporal(values, formats, column_type)

    return values


def canonical_text(typed: str, spec: dict[str, Any], formats: dict[str, str]) -> str | None:
    """Canonical output text for temporal columns; None for others."""

    column_type = spec.get("type", "string")
    if column_type not in TEMPORAL_TYPES:
        return None
    return format_temporal(typed, formats.get(column_type, CANONICAL_FORMATS[column_type]))
