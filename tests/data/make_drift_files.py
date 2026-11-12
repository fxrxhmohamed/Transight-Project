from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "data" / "raw" / "banking" / "banking_transactions.csv"
OUT = ROOT / "data" / "demo"
OUT.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(SRC, dtype=str, keep_default_na=False, nrows=120_000)
N = 20_000
part = lambda i: df.iloc[i * N:(i + 1) * N].copy()

normal = part(1)

channel = part(2)
channel.loc[channel.index[:500], "channel"] = "Crypto Wallet"

nulls = part(3)
nulls.loc[nulls.index[: int(N * 0.3)], "email"] = ""

amount = part(4)
num = pd.to_numeric(amount["transaction_amount"], errors="coerce")
amount.loc[num.notna(), "transaction_amount"] = (num[num.notna()] * 3).round(2).astype(str)

half = part(5).iloc[:8_000]

for name, frame in [("01_baseline", part(0)), ("02_normal", normal), ("03_new_channel", channel),
                    ("04_null_spike", nulls), ("05_amount_x3", amount), ("06_half_rows", half)]:
    frame.to_csv(OUT / f"{name}.csv", index=False)
    print(name, len(frame))