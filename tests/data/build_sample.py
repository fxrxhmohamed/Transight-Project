"""Build the test dataset: clean rows plus one example of each defect.

    python tests/data/build_sample.py --run ING_20261012_001

Takes rows of an ingested and validated run (read from the database:
bronze.<dataset>, dq.<dataset>_valid, dq.remediation_audit) that passed
without any
repair or warning (one per customer and account, so cross-row rules do
not interact), then derives one defective row per defect below. Writes

    tests/data/banking_sample.csv     the rows, in the source layout
    tests/data/expected_defects.csv   row number, defect, the rule that
                                      must catch it and the outcome:
                                      repaired | flagged | quarantined |
                                      removed

The test suite checks every line of expected_defects.csv. Run this again
only to change the dataset; the CSVs are committed and CI does not need
the source data.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
DATASET = "banking_transactions"
CLEAN_ROWS = 20

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "services" / "dq-service" / "src"))
from dq import load_env  # noqa: E402
from layout import Layout  # noqa: E402
from db import bronze  # noqa: E402


def clean_rows(run: str, count: int) -> pd.DataFrame:
    with bronze.connect() as conn:
        ingestion = bronze.find_ingestion(conn, DATASET, run)
        raw = bronze.read_frame(conn, ingestion)
    prefix, width = ingestion["record_id"]["prefix"], ingestion["record_id"]["width"]
    raw.insert(0, "record_id", [
        f"{prefix}{n:0{width}d}" for n in range(1, len(raw) + 1)
    ])

    layout = Layout(DATASET, run)
    valid = layout.load_valid()
    audit = layout.load_audit(1)
    passed = set(valid.loc[valid["dq_warnings"].isna(), "record_id"]) - set(audit["record_id"])

    rows = raw[raw["record_id"].isin(passed)]
    # Variety (several channels and types) and no shared customer/account.
    rows = rows.drop_duplicates("customer_id").drop_duplicates("account_id")
    picked = rows.groupby(["channel", "transaction_type"], group_keys=False).head(4)
    return picked.drop(columns="record_id").reset_index(drop=True)


def build(run: str) -> tuple[pd.DataFrame, list[dict]]:
    base = clean_rows(run, CLEAN_ROWS)
    used: set[int] = set()
    rows: list[dict] = []
    expected: list[dict] = []
    next_id = [9_000_000_000]

    def take(**where: str) -> dict:
        """Next unused clean row matching the conditions."""

        for index in base.index:
            if index in used:
                continue
            row = base.loc[index].to_dict()
            if all(row[k] == v for k, v in where.items()):
                used.add(index)
                return row
        raise ValueError(f"No clean row left with {where}")

    def new_id() -> str:
        next_id[0] += 1
        return f"TXN{next_id[0]:010d}"

    def add(row: dict, defect: str = "", rule: str = "", outcome: str = "") -> int:
        rows.append(row)
        number = len(rows)
        if defect:
            expected.append({"row": number, "defect": defect,
                             "rule_id": rule, "outcome": outcome})
        return number

    def twelve_hour(time: str) -> str:
        return datetime.strptime(time, "%H:%M:%S").strftime("%I:%M:%S %p")

    def day_first(date: str) -> str:
        return datetime.strptime(date, "%Y-%m-%d").strftime("%d/%m/%Y")

    # ---- Clean rows ----
    for _ in range(CLEAN_ROWS):
        add(take())

    # ---- Repaired automatically (record stays valid) ----
    r = take(); r["transaction_type"] = f"  {r['transaction_type']}  "
    add(r, "Spaces around a value", "DQ-PRE-001", "repaired")

    r = take(transaction_status="Completed", failure_reason="")
    r["failure_reason"] = "N/A"
    add(r, "Null placeholder 'N/A'", "DQ-PRE-002", "repaired")

    r = take(); r["customer_segment"] = r["customer_segment"].upper()
    add(r, "Wrong capitals in a category", "SCH-customer_segment-allowed_values", "repaired")

    r = take(); r["transaction_date"] = day_first(r["transaction_date"])
    add(r, "Date as DD/MM/YYYY", "SCH-transaction_date-type", "repaired")

    r = take(); r["transaction_time"] = twelve_hour(r["transaction_time"])
    add(r, "Time on a 12-hour clock", "SCH-transaction_time-type", "repaired")

    r = take(); r["annual_income"] = f"{float(r['annual_income']):,.2f}"
    add(r, "Number with thousands separators", "SCH-annual_income-type", "repaired")

    r = take(); r["bank_id"] = r["bank_id"].replace("BNK", "BNK-")
    add(r, "ID with a separator", "SCH-bank_id-pattern", "repaired")

    r = take(); r["customer_last_name"] = r["customer_last_name"] + "1"
    add(r, "Digit in a name", "SCH-customer_last_name-pattern", "repaired")

    r = take(); r["phone_number"] = "+20 " + r["phone_number"][1:4] + " " + r["phone_number"][4:]
    add(r, "Phone number in international format", "DQ-CUS-001", "repaired")

    r = take()
    add(r)
    add(dict(r), "Exact copy of the previous row", "DQ-PRE-003", "removed")

    # ---- Accepted with a warning ----
    r = take(); r["email"] = r["email"].replace("@", "@@")
    add(r, "Email with two @", "SCH-email-pattern", "flagged")

    r = take(); r["phone_number"] = "abc#123"
    add(r, "Phone number that cannot be repaired", "DQ-CUS-001", "flagged")

    r = take(transaction_type="Deposit"); r["transaction_fee"] = "15.00"
    add(r, "Fee on a deposit", "DQ-VAL-001", "flagged")

    r = take(channel="ATM"); r["card_type"] = ""
    add(r, "ATM transaction without a card type", "DQ-CMP-005", "flagged")

    r = take()
    r["customer_since_date"] = "2015-01-01"
    r["date_of_birth"] = "2000-01-01"
    r["account_open_date"] = "2016-01-01"
    add(r, "Customer younger than 18 when the relationship started", "DQ-TML-003", "flagged")

    r = take()
    add(r)
    add(dict(r, transaction_id=new_id()))
    add(dict(r, transaction_id=new_id(),
             gender="Female" if r["gender"] == "Male" else "Male"),
        "Customer with a different gender than on their other records",
        "DQ-REL-002", "flagged")

    # ---- Quarantined ----
    r = take(); r["customer_id"] = ""
    add(r, "Missing customer ID", "SCH-customer_id-not_null", "quarantined")

    r = take(); r["credit_score"] = "0"
    add(r, "Credit score outside 300-850", "SCH-credit_score-range", "quarantined")

    r = take(); r["transaction_type"] = "Teleport"
    add(r, "Unknown transaction type", "SCH-transaction_type-allowed_values", "quarantined")

    r = take(); r["transaction_amount"] = "abc"
    add(r, "Amount that is not a number", "SCH-transaction_amount-type", "quarantined")

    r = take(); r["transaction_date"] = "2099-01-01"
    add(r, "Transaction date in the future", "SCH-transaction_date-range", "quarantined")

    r = take(); r["account_id"] = "XYZ"
    add(r, "Account ID in an unknown format", "SCH-account_id-pattern", "quarantined")

    r = take(); r["branch_id"] = "BR999"
    add(r, "Branch that does not exist", "SCH-branch_id-reference", "quarantined")

    r = take(); r["account_open_date"] = "2099-12-31"
    add(r, "Transaction before the account was opened", "DQ-TML-001", "quarantined")

    r = take()
    add(r, "Transaction ID used twice (first)", "SCH-transaction_id-unique", "quarantined")
    copy = dict(r, transaction_amount=f"{float(r['transaction_amount']) + 1:.2f}")
    add(copy, "Transaction ID used twice (second)", "SCH-transaction_id-unique", "quarantined")

    r = take()
    r["occupation"] = f"  {r['occupation']}  "
    r["transaction_time"] = twelve_hour(r["transaction_time"])
    r["customer_segment"] = r["customer_segment"].upper()
    add(r, "Three values repaired automatically", "DQ-REV-001", "quarantined")

    return pd.DataFrame(rows, columns=base.columns), expected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run", required=True,
                        help="A validated ingestion to take clean rows from.")
    args = parser.parse_args()
    load_env(ROOT / ".env")

    sample, expected = build(args.run)
    sample.to_csv(HERE / "banking_sample.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    pd.DataFrame(expected).to_csv(HERE / "expected_defects.csv", index=False)
    print(f"{len(sample)} rows, {len(expected)} defects -> {HERE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
