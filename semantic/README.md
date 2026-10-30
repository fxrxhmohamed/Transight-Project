# Semantic layer: fact table measures

The Power BI semantic model over `gold.fact_transaction` and its
dimensions. Every measure here is also a metric of the semantic layer
(`dbt/models/semantic/_metrics.yml`, same name in snake case), computed
by `python dq.py semantic` and stored in PostgreSQL
(`semantic.metric_report`); these DAX versions give the same numbers in
Power BI for any slice (channel, segment, branch...). The measures answer the project's business question: **how can
the bank serve its transactions more cheaply and reliably by moving
customers from assisted channels (ATM, Branch, Call Center, POS) to
digital ones (Mobile App, Internet Banking, InstaPay)?** In the data, an
assisted transaction costs the bank about 16 EGP to serve and a digital one
about 1 EGP.

| File | What |
|---|---|
| `fact_transaction_measures.dax` | every measure, ready for Power BI's DAX query view |
| `measure_checks.sql` | the headline measures in SQL, to check Power BI against the database |

## Set up

1. Load the gold tables (`gold.fact_transaction`, `gold.dim_*`) and name
   them after the gold tables (`fact_transaction`, `dim_customer`, ...).
2. Relationships, all many-to-one from the fact table:
   `transaction_date -> dim_date[date_key]`, `customer_id`, `account_id`,
   `branch_id`, `bank_id` to their dimensions. Do not also relate
   `dim_account -> dim_customer` or `dim_branch -> dim_bank`: the fact
   table already carries those keys, and a second path makes the model
   ambiguous.
3. Mark `dim_date` as the date table (`date_key`).
4. Open DAX query view, paste `fact_transaction_measures.dax`, choose
   **Update model: Add new measures**, then **Run**. Set each measure's
   display folder and format from the table below.
5. Check: with no filters, the `EVALUATE` row must equal the first row
   of `measure_checks.sql` (run with psql on the same database).

## Rules the measures follow

- **Money is summed in EGP only.** `amount_egp` and
  `estimated_channel_cost_egp` are in EGP. `transaction_amount` and
  `transaction_fee` are in the transaction's currency (about 6% of rows
  are USD, EUR, SAR or AED), so they are never summed directly. Fees are
  converted with each row's own rate (`amount_egp / transaction_amount`).
- **"Value" means completed transactions.** "Attempted value" also
  includes failed, pending and reversed ones.
- **Ratios are sum over sum** (`DIVIDE`), never an average of row
  ratios, so they add up correctly across any slice.
- **`balance_after_transaction` has no measure.** It is a point-in-time
  balance in the account's currency, so summing it means nothing.
- **Transfers count as outgoing money**, because the data has no
  transfer direction.

## Measures

Format codes: count `#,0`; EGP `#,0` (per-transaction EGP `#,0.00`); rate `0.0%`.

### 1 Volume & Value: how much business goes through the bank?

| Measure | Definition | Format |
|---|---|---|
| Transactions | all transactions | count |
| Completed Transactions | status Completed | count |
| Attempted Value (EGP) | amount in EGP, every status | EGP |
| Transaction Value (EGP) | amount in EGP, completed | EGP |
| Avg / Median Transaction Value (EGP) | per completed transaction | EGP |
| Inflow / Outflow Value (EGP) | deposits and interest credits / withdrawals, purchases, transfers, bill payments, fees, loan repayments | EGP |
| Net Flow (EGP) | inflow - outflow | EGP |
| Foreign Currency Value Share % | value not in EGP / value | rate |

### 2 Revenue: what do transactions earn, and does it cover their cost?

| Measure | Definition | Format |
|---|---|---|
| Fee Revenue (EGP) | fees of completed transactions, converted to EGP | EGP |
| Fee-Bearing Transactions | completed transactions with a fee | count |
| Avg Fee per Fee-Bearing Transaction (EGP) | fee revenue / fee-bearing transactions | EGP |
| Fee Yield % | fee revenue / value of fee-bearing transactions | rate |
| Fee Coverage of Channel Cost % | fee revenue / channel cost | rate |

### 3 Channel Cost: what does serving transactions cost?

| Measure | Definition | Format |
|---|---|---|
| Channel Cost (EGP) | sum of the estimated channel cost | EGP |
| Cost per Transaction (EGP) | channel cost / transactions | EGP |
| Cost per 1K EGP Processed | channel cost per 1,000 EGP of value | EGP |
| Digital / Assisted Cost per Transaction (EGP) | benchmark cost of each channel kind, ignoring the visual's channel filters | EGP |
| Assisted vs Digital Cost Multiple | assisted cost / digital cost | `0.0"x"` |
| Cost of Failed Transactions (EGP) | channel cost spent on failures | EGP |

### 4 Digital Adoption: how far have customers moved to digital?

| Measure | Definition | Format |
|---|---|---|
| Digital / Assisted Transactions | by `digital_channel_flag` | count |
| Digital Share % | digital / all transactions | rate |
| Digital Value (EGP), Digital Value Share % | completed digital value, and its share of value | EGP, rate |
| Digitally Active Customers | customers with a digital transaction | count |
| Digital Adoption Rate % | digitally active / active customers | rate |
| Assisted-Only Customers | customers with no digital transaction: the onboarding audience | count |
| Digital Login Success Rate % | successful logins / digital transactions with a login | rate |
| Avg Digital Session (min) | session length on digital channels | `0.0` |

### 5 Migration Opportunity: what would moving to digital save?

`digital_migration_opportunity` marks transfers, bill payments and loan
repayments made at an ATM, branch or call center: these could have been
made digitally.

| Measure | Definition | Format |
|---|---|---|
| Migration Opportunities | flagged transactions | count |
| Migration Opportunity Rate % | opportunities / assisted transactions | rate |
| Migration Opportunity Value (EGP) | completed value of the opportunities | EGP |
| Customers with Migration Opportunity | customers to target | count |
| Migration Opportunity Cost (EGP) | what the opportunities cost today | EGP |
| Potential Cost Savings (EGP) | opportunity cost - opportunities x digital cost per transaction | EGP |
| Potential Savings % of Channel Cost | savings / channel cost | rate |
| Savings per Migrated Customer (EGP) | savings / customers with an opportunity | EGP |
| Digital Share if Migrated % | digital share once every opportunity has moved (the target) | rate |

### 6 Reliability: how often do transactions fail, why, and what is lost?

| Measure | Definition | Format |
|---|---|---|
| Failed / Reversed / Pending Transactions | by status | count |
| Success / Failure / Reversal / Pending Rate % | status / all transactions | rate |
| Failed Value (EGP) | attempted value of failed transactions | EGP |
| Failure Reason Share % | each reason's part of the failures (`failure_reason` on the axis) | rate |
| Technical Failure Rate % | network timeouts / transactions (fixable by the bank) | rate |
| Authentication Failure Rate %, Insufficient Funds Rate % | those failures / transactions | rate |
| Avg Attempts per Transaction | mean of `attempt_count` | `0.00` |
| Retried Transactions % | more than one attempt / transactions | rate |
| First-Attempt Success Rate % | completed on the first attempt / transactions | rate |

### 7 Customers: who transacts, how often and for how much?

| Measure | Definition | Format |
|---|---|---|
| Active Customers, Active Accounts | distinct customers / accounts in the selection | count |
| Transactions per Customer | transactions / active customers | `0.0` |
| Value per Customer (EGP) | value / active customers | EGP |
| Fee Revenue / Channel Cost per Customer (EGP) | per active customer | EGP |

Slice them by `dim_customer[customer_segment]`, `occupation`,
`customer_governorate`, and so on. For example, comparing digital adoption
by segment shows where onboarding campaigns should start.

### 8 Payments & Network: where is money spent, and through what?

| Measure | Definition | Format |
|---|---|---|
| Purchase Value (EGP), Avg Purchase Ticket (EGP) | completed purchases | EGP |
| Bill Payment Value (EGP) | completed bill payments | EGP |
| Active Merchants | distinct merchants | count |
| Card Transactions, Card Share % | transactions with a card | count, rate |
| Active Branches, Transactions per Branch | branch network load | count, `#,0` |

### 9 Trends: are we growing, and is adoption accelerating?

| Measure | Definition | Format |
|---|---|---|
| Transaction Value YTD / PY / YoY % / R12M | year to date, same period last year, growth, rolling 12 months | EGP, rate |
| Transactions PY, YoY %, MoM % | volume growth | count, rate |
| Digital Share PY %, Digital Share YoY Change (pp) | adoption trend, in percentage points | rate |
| Active Customers PY, YoY % | customer base growth | count, rate |
| Channel Cost YoY % | is cost falling as adoption rises? | rate |

### 10 Data Freshness: how current is the report?

| Measure | Definition | Format |
|---|---|---|
| Latest Transaction Date | last business date in gold | date |
| Load Runs in Data | ingestions the current data comes from (full + incremental) | count |
| Latest Load Run | newest ingestion id in gold | text |

Data-quality scores (initial and final DQ score, auto-fix rate, and so on)
come from the semantic layer (`semantic.metric_report`, area `Data
Quality`) and the DQ tables (`semantic.dq_*`), not the fact table: see
"Data quality metrics" in the main README.

## Suggested report pages

| Page | Key measures | Slice by |
|---|---|---|
| Executive overview | Transaction Value, Transactions, Fee Revenue, Channel Cost, Digital Share %, Potential Cost Savings | year, bank |
| Digital migration | Digital Share %, Digital Adoption Rate %, Assisted-Only Customers, Migration Opportunities, Potential Cost Savings, Digital Share if Migrated % | channel, transaction type, segment, governorate |
| Channel economics | Cost per Transaction, Assisted vs Digital Cost Multiple, Fee Coverage of Channel Cost %, Cost per 1K EGP Processed | channel, month |
| Reliability | Failure Rate %, Failure Reason Share %, Technical Failure Rate %, First-Attempt Success Rate %, Failed Value | channel, failure reason, month |
| Customers | Active Customers, Value per Customer, Transactions per Customer, Digital Adoption Rate % | segment, occupation, governorate |
