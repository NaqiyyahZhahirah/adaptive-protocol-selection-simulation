import sys
from pathlib import Path

import numpy as np
import pandas as pd

HIGHER_BETTER = {"pdr_pct": True, "latency_mean": False, "latency_p95": False,
                 "overhead_pct": False, "bytes_per_delivered": False}


def load(mode):
    d = Path("results") / mode
    return pd.read_csv(d / "messages.csv"), pd.read_csv(d / "runs.csv")


def run_metrics(msgs, runs):
    lat = msgs.groupby("run_id")["latency_ms"].agg(
        latency_mean="mean", latency_p95=lambda s: s.quantile(0.95))
    df = runs.set_index("run_id").join(lat).reset_index()
    df["pdr_pct"] = 100 * df.n_recv / df.n_sent
    df["overhead_pct"] = 100 * (df.wire_total - df.payload_bytes_total) / df.wire_total
    df["bytes_per_delivered"] = df.wire_total / df.n_recv.replace(0, np.nan)
    return df


def category_metrics(msgs, runs):
    # pakai kondisi run ("dynamic" utuh), bukan kondisi blok per pesan
    m = msgs.drop(columns="condition").merge(runs[["run_id", "condition"]], on="run_id")
    g = m.groupby(["run_id", "scenario", "condition", "seed", "category"])
    return g.agg(pdr_pct=("success", lambda s: 100 * s.mean()),
                 latency_mean=("latency_ms", "mean"),
                 latency_p95=("latency_ms", lambda s: s.quantile(0.95))).reset_index()

def estimated_overhead_by_category(msgs_comp, df_char):
    """Estimasi, bukan pengukuran."""
    ov = df_char.groupby("scenario")["overhead_pct"].mean()
    ad = msgs_comp[msgs_comp.scenario == "adaptive"]
    share = pd.crosstab(ad.category, ad.protocol, normalize="index")
    return (share * ov.reindex(share.columns)).sum(axis=1).round(1)

def compare(df, keys, base, test="adaptive"):
    """Δ% dipasangkan per seed. n_better = jumlah seed di mana adaptif lebih baik."""
    cols = [c for c in HIGHER_BETTER if c in df.columns]
    idx = keys + ["seed"]
    a = df[df.scenario == test].set_index(idx)[cols]
    b = df[df.scenario == base].set_index(idx)[cols]
    d = 100 * (a - b) / b.replace(0, np.nan)
    out = []
    for m in cols:
        better = (d[m] > 0) if HIGHER_BETTER[m] else (d[m] < 0)
        s = (pd.DataFrame({"delta": d[m], "better": better})
             .groupby(level=keys)
             .agg(delta_mean=("delta", "mean"), delta_std=("delta", "std"),
                  n_better=("better", "sum"), n=("better", "size")))
        s.insert(0, "metric", m)
        out.append(s)
    return pd.concat(out).reset_index().round(2)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "comparative"
    msgs, runs = load(mode)
    df = run_metrics(msgs, runs)
    pd.set_option("display.width", 200)
    print(df.groupby(["scenario", "condition"])[list(HIGHER_BETTER)].agg(["mean", "std"]).round(2))
    if "adaptive" in set(df.scenario):
        cat = category_metrics(msgs, runs)
        for base in ("mqtt_qos1", "coap_con"):
            print(f"\n=== Δ% adaptif vs {base}, per kondisi ===")
            print(compare(df, ["condition"], base).to_string(index=False))
            print(f"\n=== Δ% adaptif vs {base}, per kondisi dan kategori ===")
            print(compare(cat, ["condition", "category"], base).to_string(index=False))
        ad = msgs[msgs.scenario == "adaptive"]
        print("\n=== Distribusi keputusan adaptif per kategori (%) ===")
        print((pd.crosstab(ad.category, ad.protocol, normalize="index") * 100).round(1))
        # Estimasi overhead adaptif berdasarkan kategori
        df_char = df[df.scenario == "char"].copy()
        msgs_comp = msgs.copy()
        est_ov = estimated_overhead_by_category(msgs_comp, df_char)
        print("\n=== Estimasi overhead adaptif per kategori (%) ===")
        print(est_ov.to_string())