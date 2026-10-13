import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from analysis.metrics import HIGHER_BETTER, compare, load, run_metrics

OUT = Path("results") / "figures"
ORDER = ["mqtt_qos0", "mqtt_qos1", "mqtt_qos2", "coap_non", "coap_con", "adaptive"]
LV = ["low", "medium", "high"]
COND_ORDER = [f"d{d}_l{l}" for d in LV for l in LV] + ["dynamic"]
LABEL = {"pdr_pct": "PDR (%)", "latency_mean": "Latency rata-rata (ms)",
         "latency_p95": "Latency P95 (ms)", "overhead_pct": "Overhead (%)",
         "bytes_per_delivered": "Byte per pesan sampai"}


def _save(fig, name):
    OUT.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUT / name, dpi=200)
    plt.close(fig)


def _order(df):
    conds = [c for c in COND_ORDER if c in set(df.condition)]
    scs = [s for s in ORDER if s in set(df.scenario)]
    return conds, scs


def plot_metric_bars(df, tag):
    """Rata-rata ± std antar seed, per kondisi dan skenario (RM1 dan RM3)."""
    conds, scs = _order(df)
    metrics = [m for m in HIGHER_BETTER if m in df.columns]
    fig, axes = plt.subplots(len(metrics), 1, figsize=(13, 3.2 * len(metrics)), sharex=True)
    for i, (ax, m) in enumerate(zip(axes, metrics)):
        g = df.groupby(["scenario", "condition"])[m].agg(["mean", "std"])
        mean = g["mean"].unstack(0).reindex(conds)[scs]
        std = g["std"].unstack(0).reindex(conds)[scs]
        mean.plot.bar(ax=ax, yerr=std, capsize=2, width=0.85, legend=(i == 0))
        ax.set_ylabel(LABEL[m])
        ax.set_xlabel("")
    axes[-1].set_xticklabels(conds, rotation=30, ha="right")
    _save(fig, f"{tag}_metrics_bar.png")


def plot_delta(df, tag):
    """Δ% adaptif vs statis. Hijau = adaptif lebih baik, merah = lebih buruk."""
    conds, _ = _order(df)
    metrics = [m for m in HIGHER_BETTER if m in df.columns]
    for base in ("mqtt_qos1", "coap_con"):
        d = compare(df, ["condition"], base)
        fig, axes = plt.subplots(len(metrics), 1, figsize=(12, 2.8 * len(metrics)), sharex=True)
        for ax, m in zip(axes, metrics):
            s = d[d.metric == m].set_index("condition").reindex(conds)
            good = (s.delta_mean > 0) if HIGHER_BETTER[m] else (s.delta_mean < 0)
            ax.bar(range(len(conds)), s.delta_mean, yerr=s.delta_std, capsize=2,
                   color=["tab:green" if g else "tab:red" for g in good])
            ax.axhline(0, color="k", lw=0.8)
            ax.set_ylabel(f"Δ% {LABEL[m]}")
        axes[-1].set_xticks(range(len(conds)))
        axes[-1].set_xticklabels(conds, rotation=30, ha="right")
        fig.suptitle(f"Adaptif vs {base}")
        _save(fig, f"{tag}_delta_vs_{base}.png")


def _with_run_condition(msgs, runs):
    return msgs.drop(columns="condition").merge(runs[["run_id", "condition"]], on="run_id")


def plot_latency_box(msgs, runs, tag):
    m = _with_run_condition(msgs, runs).dropna(subset=["latency_ms"])
    conds, scs = _order(m)
    ncol = 5
    nrow = -(-len(conds) // ncol)
    fig, axes = plt.subplots(nrow, ncol, figsize=(4 * ncol, 3.6 * nrow), squeeze=False)
    for ax, c in zip(axes.ravel(), conds):
        sub = m[m.condition == c]
        ax.boxplot([sub[sub.scenario == s].latency_ms for s in scs],
                   showfliers=False)
        ax.set_xticklabels(scs, rotation=60, ha="right", fontsize=8)
        ax.set_title(c, fontsize=10)
    for ax in axes.ravel()[len(conds):]:
        ax.axis("off")
    fig.supylabel("Latency (ms)")
    _save(fig, f"{tag}_latency_box.png")


def plot_latency_cdf(msgs, runs, tag):
    m = _with_run_condition(msgs, runs).dropna(subset=["latency_ms"])
    panels = [("dynamic", m[m.condition == "dynamic"]),
              ("semua kondisi statis (digabung)", m[m.condition != "dynamic"])]
    panels = [(t, d) for t, d in panels if len(d)]
    fig, axes = plt.subplots(1, len(panels), figsize=(6.5 * len(panels), 4.5), squeeze=False)
    for ax, (title, d) in zip(axes[0], panels):
        for s in [s for s in ORDER if s in set(d.scenario)]:
            x = d[d.scenario == s].latency_ms.sort_values().to_numpy()
            ax.step(x, (pd.Series(range(1, len(x) + 1)) / len(x)).to_numpy(), label=s, where="post")
        ax.set_xscale("log")
        ax.set_xlabel("Latency (ms, skala log)")
        ax.set_ylabel("CDF")
        ax.set_title(title)
        ax.legend()
    _save(fig, f"{tag}_latency_cdf.png")


def plot_decisions(msgs, tag):
    ad = msgs[msgs.scenario == "adaptive"]
    ct = pd.crosstab(ad.category, ad.protocol, normalize="index") * 100
    ax = ct.plot.bar(stacked=True, figsize=(6, 4), rot=0)
    ax.set_ylabel("Keputusan (%)")
    ax.set_xlabel("Kategori kritikalitas")
    _save(ax.figure, f"{tag}_decisions.png")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "comparative"
    msgs, runs = load(mode)
    df = run_metrics(msgs, runs)
    plot_metric_bars(df, mode)
    plot_latency_box(msgs, runs, mode)
    plot_latency_cdf(msgs, runs, mode)
    if "adaptive" in set(df.scenario):
        plot_delta(df, mode)
        plot_decisions(msgs, mode)
    print("Gambar disimpan di", OUT)