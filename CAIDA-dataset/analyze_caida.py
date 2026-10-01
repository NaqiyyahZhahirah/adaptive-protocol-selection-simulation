import json
import numpy as np
import pandas as pd

CSV = "results/traces_parsed.csv"
WINDOW_SECONDS = 3600
MIN_TRACES = 200
LOSS_METHOD = "B"

df = pd.read_csv(CSV, dtype={"monitor": "category", "replied": "int8"})
df["window"] = df["ts"] // WINDOW_SECONDS

rtt = df.loc[(df.replied == 1) & (df.rtt_ms > 0), "rtt_ms"].astype("float32")

g = df.groupby(["monitor", "window"], observed=True)
win = pd.DataFrame({
    "n": g.size(),
    "loss_A": 100 * (1 - g["replied"].mean()),
    "tries_total": g["tries_total"].sum(),
    "tries_lost": g["tries_lost"].sum(),
})
win["loss_B"] = 100 * win["tries_lost"] / win["tries_total"].replace(0, np.nan)
win = win[win["n"] >= MIN_TRACES].reset_index()
loss = win[f"loss_{LOSS_METHOD}"].dropna()

q = [25, 50, 75, 95, 99]
rtt_q = dict(zip(q, np.percentile(rtt, q)))
loss_q = dict(zip(q, np.percentile(loss, q)))
LOSS_CAP = 30.0 

print(f"Trace: {len(df):,} | Monitor: {df['monitor'].nunique()} | Jendela valid: {len(win)}")
print("Periode:", pd.to_datetime(df.ts.min(), unit="s", utc=True),
      "s/d", pd.to_datetime(df.ts.max(), unit="s", utc=True))
print(f"Respons tujuan (Replied): {df.replied.mean()*100:.1f}%")
if min(loss_q[99], LOSS_CAP) <= loss_q[75]:
    print("PERINGATAN: batas 'Tinggi' <= P75. Pakai rentang tetap 0-5 / 5-15 / 15-30 %.")
print(f"RTT: n={len(rtt):,} | P25={rtt_q[25]:.1f} P50={rtt_q[50]:.1f} "
      f"P75={rtt_q[75]:.1f} P95={rtt_q[95]:.1f} P99={rtt_q[99]:.1f} ms")
print(f"Loss ({LOSS_METHOD}): P25={loss_q[25]:.2f} P50={loss_q[50]:.2f} "
      f"P75={loss_q[75]:.2f} P99={loss_q[99]:.2f} %")
print(f"Jendela loss=0: {(loss == 0).mean() * 100:.1f}% | loss>30%: {(loss > LOSS_CAP).mean() * 100:.1f}%")
print("Perbandingan metode: loss_A median =", round(win['loss_A'].median(), 2),
      "| loss_B median =", round(win['loss_B'].median(), 2))

tabel = pd.DataFrame([
    ("Rendah", f"< {rtt_q[25]:.1f}", f"< {loss_q[25]:.2f}"),
    ("Sedang", f"{rtt_q[25]:.1f} – {rtt_q[75]:.1f}", f"{loss_q[25]:.2f} – {loss_q[75]:.2f}"),
    ("Tinggi", f"{rtt_q[75]:.1f} – {rtt_q[99]:.1f}",
     f"{loss_q[75]:.2f} – {min(loss_q[99], LOSS_CAP):.2f}"),
], columns=["Tingkat", "Delay/RTT (ms)", "Packet loss (%)"])
print("\nTabel 3.2\n", tabel.to_string(index=False))

profile = {
    "window_seconds": WINDOW_SECONDS, "min_traces": MIN_TRACES, "loss_method": LOSS_METHOD,
    "rtt_p25": rtt_q[25], "rtt_p50": rtt_q[50], "rtt_p75": rtt_q[75], "rtt_p99": rtt_q[99],
    "loss_p25": loss_q[25], "loss_p50": loss_q[50], "loss_p75": loss_q[75], "loss_p99": loss_q[99],
}
json.dump(profile, open("results/network_profile.json", "w"), indent=2)
np.save("results/rtt_samples.npy", rtt.to_numpy())
win.to_csv("results/loss_windows.csv", index=False)