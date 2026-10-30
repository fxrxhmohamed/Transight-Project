# Transight Banking report (Power BI project)

A Power BI Project (PBIP: TMDL semantic model + PBIR report) over the
PostgreSQL `gold` star schema and the `semantic.dq_*` tables. Open
`Transight Banking.pbip` in Power BI Desktop, then **Home → Refresh**: the
model is import mode and ships without data.

| Path | What |
|---|---|
| `Transight Banking.pbip` | open this |
| `Transight Banking.SemanticModel/` | tables, relationships, 120 measures (TMDL) |
| `Transight Banking.Report/` | pages and visuals (PBIR), theme in `StaticResources/` |
| `tools/build_pbip.py` | generates both from the repository (see below) |
| `validation/reconcile.sql`, `reconcile.dax` | the same figures from PostgreSQL and from the model |

## Connection

Parameters `PgServer` (`localhost:5432`) and `PgDatabase` (`metaflow`):
**Transform data → Edit parameters**. The first refresh asks for the
PostgreSQL login (`POSTGRES_USER` / `POSTGRES_PASSWORD` in `.env`).

## Model

| Table | Source | Grain |
|---|---|---|
| `fact_transaction` | `gold.fact_transaction` (+ `channel_group` Digital/Assisted from `digital_channel_flag`) | transaction |
| `dim_date` | `gold.dim_date`, **marked as date table** | day, 2022-01-01 – 2025-12-31 |
| `dim_customer` | `gold.dim_customer` without names, e-mail, phone, birth date; + `age_band` (age on the last data day) and `income_band` (quintiles of `annual_income`) | customer |
| `dim_account`, `dim_branch`, `dim_bank` | `gold.dim_*` | key |
| `dq_runs` | `semantic.dq_runs` | run (latest correction round) |
| `dq_run_rounds` | `semantic.dq_run` | run × round |
| `dq_rule_checks` | `semantic.dq_rule_checks` | run × rule, first validation round |
| `dq_correction_summary` | `semantic.dq_correction`, counted by run, round, type, action, outcome, rule, column | — |
| `dq_quarantine_status` | `semantic.dq_quarantine`, latest status per record, counted | run × status |

Relationships are all many-to-one, single direction: the fact to its five
dimensions (as `semantic/README.md` prescribes: no `dim_branch → dim_bank`
or `dim_account → dim_customer`), and the four DQ detail tables to `dq_runs`
on `run_id`.

Measures: the 85 of `semantic/fact_transaction_measures.dax`, unchanged
(folders 1–10), plus:

- **11 Report**: latest-year YoY for the KPI cards, card titles, header
  texts, drill-through labels.
- **DQ Records / DQ Scores / DQ Rules / DQ Corrections**: the data-quality
  metrics with the definitions of `dbt/models/semantic/_metrics.yml`
  (initial score = valid / processed, final = (valid + resolved) /
  processed, auto-fix rate = auto-fixed / records with issues, resolution
  rate = resolved / quarantined, rule failure = failed / checked, rule
  repair = repaired / failed).

## Pages

1. **Executive Banking Overview**: transactions, completed value, fee
   revenue, channel servicing cost, average value, failure rate, YoY of
   the latest year; monthly volume and value, YoY by year, status
   breakdown, bank and branch tables. Slicers: year, bank, channel, type.
2. **Digital Adoption & Cost Optimization**: digital share, adoption
   rate, assisted and eligible (migration-opportunity) transactions,
   **estimated** potential savings, target digital share, cost multiple,
   fee coverage; channel volumes, monthly adoption, segment / age /
   income / branch views. Slicers: year (synced with page 1), region,
   segment, age band, income quintile.
3. **Data Quality & Pipeline Monitoring**: initial / final score and
   uplift, processed, with issues, auto-repair rate, quarantine
   resolution rate, unresolved; record outcomes, failures and repair rate
   by DQ dimension, failures by severity, rule table, round history,
   correction outcomes. Slicers: run, dataset, DQ dimension, severity.
4. **Branch Detail** (hidden, drill-through on `dim_branch[branch_name]`
   from the branch tables of pages 1 and 2).
5. **Channel Tooltip** (hidden, report-page tooltip of the channel bar
   charts).

## Validated

- All PBIR / PBIP JSON files against Microsoft's published schemas.
- Opened in Power BI Desktop 2.158 (September 2026), refreshed from
  PostgreSQL; all 120 measures compile.
- `validation/reconcile.dax` against `validation/reconcile.sql`: every
  figure equal (transactions 1,016,522; completed value EGP
  11,033,668,288.09; fee revenue EGP 12,471,657.59; channel cost EGP
  10,536,416.46; potential savings EGP 1,784,654.23; DQ scores 92.6891% →
  92.6892%; by year, status, bank and segment as well).

## Things to know when reading the numbers

- **Savings are estimates.** `Potential Cost Savings (EGP)` = the
  estimated channel cost of the migration opportunities minus the same
  count at the digital cost per transaction (data: ≈ EGP 15.9 assisted,
  ≈ EGP 1.0 digital). Nothing has been migrated or saved.
  `estimated_channel_cost_egp` is itself an estimate, so "channel cost"
  and "fee coverage" are estimates too.
- **"Assisted" includes the System and Other channels** (50,439 bank-made
  interest credits and fees, 9,786 other), because it follows
  `digital_channel_flag`. Their estimated cost is counted in the channel
  cost, which lowers the digital share and raises the cost of "assisted".
- **Digital adoption rate is 99% over four years**: almost every customer
  used a digital channel at least once. Read it per month (page 2 trend,
  ≈ 40–45%) or per segment, not as a total.
- **Digital share barely varies by segment, age or income (36.8–37.3%)**:
  this is what the data holds, not a report error.
- **Fee revenue** is `transaction_fee` of completed transactions converted
  at each row's rate; amounts of `Fee`-type transactions are not part of it.
- **Data quality**: one run so far (`ING_20261013_001`), one record
  resolved after quarantine, 80,178 still open, so the final score is
  only 0.0001 pp above the initial one. Rule figures are from the first
  validation round. The DQ dimension and severity slicers filter the rule
  visuals only.
- **`semantic.metric_report` is older than gold**: it was computed before
  the resolved record reached gold (1,016,521 transactions there, 1,016,522
  in gold). The report reads gold, so its numbers match gold; run
  `python dq.py semantic` to bring the stored metrics up to date.
- `semantic/fact_transaction_measures.dax` names a variable `PreviousMonth`
  in `Transactions MoM %`; PREVIOUSMONTH is a DAX function, so the measure
  fails there. The generator renames it to `PriorMonth` in this model only.

## Regenerate

```
python powerbi/tools/build_pbip.py --force
```

rebuilds the model and report from `semantic/fact_transaction_measures.dax`
and the definitions in the script. It overwrites edits made in Power BI
Desktop, so once the report is edited by hand, edit it in Desktop instead.
