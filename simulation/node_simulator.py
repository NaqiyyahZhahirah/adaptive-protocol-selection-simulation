import json
import random

import pandas as pd

from simulation import config as C


def slug(name):
    return name.lower().replace(" ", "-")


def category(v):
    if v <= C.WHO_AQG:
        return "normal"
    return "warning" if v <= C.WHO_IT3 else "critical"


def load_points():
    """Rata-rata bergulir 24 jam per node (dengan riwayat sebelumnya)."""
    df = pd.read_csv(C.PAYLOAD_CSV)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    out = {}
    for name, g in df.groupby("location"):
        s = g.sort_values("timestamp").set_index("timestamp")["pm25"]
        roll = s.rolling("24h", min_periods=C.MIN_PERIODS).mean()
        pts = pd.DataFrame({"pm25": s, "pm25_24h": roll}).dropna()
        pts["category"] = pts["pm25_24h"].map(category)
        out[name] = pts
    return out


def build_messages(data_seed=C.DATA_SEED, limit=None):
    """Sampel berstrata (node x kategori), tetap untuk semua run,
    diurutkan kronologis per node."""
    pts = load_points()
    rng = random.Random(data_seed)
    out = {}
    for ni, node in enumerate(C.NODES):
        g, parts = pts[node], []
        for cat, n in C.ALLOCATION[node].items():
            if n == 0:
                continue
            pool = g[g.category == cat]
            if len(pool) < n:
                raise ValueError(f"{node}/{cat}: tersedia {len(pool)}, butuh {n}")
            parts.append(pool.iloc[sorted(rng.sample(range(len(pool)), n))])
        sel = pd.concat(parts).sort_index()
        msgs = []
        for seq, (ts, row) in enumerate(sel.iterrows()):
            mid = ni * 1000 + seq
            payload = json.dumps(
                {"id": slug(node), "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                 "pm25": round(float(row.pm25), 1), "m": mid},
                separators=(",", ":")).encode()
            msgs.append({"msg_id": mid, "seq": seq, "category": row.category,
                         "pm25_hourly": float(row.pm25),
                         "pm25_24h": float(row.pm25_24h), "payload": payload})
        out[node] = msgs[:limit] if limit else msgs
    return out


if __name__ == "__main__":
    from collections import Counter
    ms, tot = build_messages(), Counter()
    for n, lst in ms.items():
        c = Counter(m["category"] for m in lst)
        tot.update(c)
        print(f"{n:18s} {len(lst):4d} {dict(c)}  payload~{len(lst[0]['payload'])} B")
    print("total", sum(len(v) for v in ms.values()), dict(tot))