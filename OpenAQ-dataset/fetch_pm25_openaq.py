"""
Fetch PM2.5 hourly OpenAQ - kriteria sesuai kebutuhan simulasi
(bukan evaluasi kualitas stasiun)

Kriteria seleksi:
  1. Ada sensor PM2.5 (semua sensor di lokasi digabung, bukan hanya satu).
  2. Jendela 30 hari terakhir yang tersedia per stasiun (independen antarstasiun).
  3. n_obs >= 120 -> diturunkan dari desain eksperimen sendiri:
     600 pesan/run dibagi 5 node simulator = rata-rata 120 pesan/node,
     sehingga tiap node punya cukup nilai PM2.5 riil tanpa pengulangan
     berlebihan dalam satu run.
  4. Variasi: mencakup >= 2 kategori WHO (Normal/Warning/Critical berbasis
     rata-rata 24 jam), bukan ambang rentang angka yang tidak berdasar.

completeness TIDAK dipakai sebagai syarat lolos/gagal -- tetap dihitung
dan disimpan di metadata sebagai informasi tambahan saja.
"""

import os
import time
import pandas as pd
from dotenv import load_dotenv
from openaq import OpenAQ

load_dotenv()
client = OpenAQ(api_key=os.getenv("OPENAQ_API_KEY"))

COUNTRY_CODE = "ID"
WINDOW_DAYS = 30
MIN_OBS = 120            # dari desain sendiri: 600 pesan/run / 5 node
MIN_CATEGORIES = 2        # minimal mencakup 2 dari 3 kategori WHO
N_STATIONS_MIN, N_STATIONS_MAX = 3, 5

WHO_NORMAL_MAX = 15.0
WHO_WARNING_MAX = 37.5

SLEEP = 0.3
PAGE_LIMIT = 1000
OUT_DIR = "raw"
BBOX_LAT, BBOX_LON = (-11, 6), (95, 141)
os.makedirs(OUT_DIR, exist_ok=True)


def pick(obj, *names):
    for n in names:
        v = getattr(obj, n, None)
        if v is not None:
            return v
    return None


def to_utc(dt_obj):
    if dt_obj is None:
        return None
    raw = dt_obj if isinstance(dt_obj, str) else pick(dt_obj, "utc")
    return pd.to_datetime(raw, utc=True) if raw else None


def get_indonesia_country_id():
    for c in client.countries.list(limit=1000).results:
        if c.code == COUNTRY_CODE:
            return c.id
    raise RuntimeError("Indonesia tidak ditemukan")


def get_all_locations(country_id):
    out, page = [], 1
    while True:
        res = client.locations.list(countries_id=country_id, limit=PAGE_LIMIT, page=page)
        out.extend(res.results)
        if len(res.results) < PAGE_LIMIT:
            return out
        page += 1
        time.sleep(SLEEP)


def in_bbox(loc):
    lat, lon = loc.coordinates.latitude, loc.coordinates.longitude
    return BBOX_LAT[0] <= lat <= BBOX_LAT[1] and BBOX_LON[0] <= lon <= BBOX_LON[1]


def pm25_sensors(loc):
    return [s for s in loc.sensors if s.parameter.name == "pm25"]


def fetch_hourly(sensor_id, dt_from, dt_to):
    rows, page = [], 1
    while True:
        res = client.measurements.list(
            sensors_id=sensor_id, data="hours",
            datetime_from=dt_from.isoformat(), datetime_to=dt_to.isoformat(),
            limit=PAGE_LIMIT, page=page,
        )
        for m in res.results:
            period = pick(m, "period")
            rows.append({"timestamp": to_utc(pick(period, "datetime_from")) if period else None,
                         "pm25": m.value})
        if len(res.results) < PAGE_LIMIT:
            break
        page += 1
        time.sleep(SLEEP)
    return pd.DataFrame(rows)


def fetch_hourly_merged(sensor_ids, dt_from, dt_to):
    frames = []
    for sid in sensor_ids:
        df = fetch_hourly(sid, dt_from, dt_to)
        if not df.empty:
            frames.append(df)
        time.sleep(SLEEP)
    if not frames:
        return pd.DataFrame(columns=["timestamp", "pm25"])
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.dropna(subset=["timestamp", "pm25"])
    merged = merged[merged["pm25"] >= 0]
    return merged.groupby("timestamp", as_index=False)["pm25"].mean()


def classify_by_24h_average(df_hourly):
    daily = df_hourly.set_index("timestamp")["pm25"].resample("24h").mean().dropna()
    if daily.empty:
        return daily, {"pct_normal": None, "pct_warning": None, "pct_critical": None}
    stats = {
        "pct_normal": round((daily <= WHO_NORMAL_MAX).mean() * 100, 1),
        "pct_warning": round(((daily > WHO_NORMAL_MAX) & (daily <= WHO_WARNING_MAX)).mean() * 100, 1),
        "pct_critical": round((daily > WHO_WARNING_MAX).mean() * 100, 1),
    }
    return daily, stats


def profile_station(loc):
    sensors = pm25_sensors(loc)
    if not sensors:
        return None

    last = to_utc(pick(loc, "datetime_last"))
    first = to_utc(pick(loc, "datetime_first"))
    if last is None or first is None:
        return None

    window_end = min(last, pd.Timestamp.now(tz="UTC"))
    window_start = max(window_end - pd.Timedelta(days=WINDOW_DAYS), first)

    df = fetch_hourly_merged([s.id for s in sensors], window_start, window_end)
    if df.empty:
        return None

    daily, who_stats = classify_by_24h_average(df)
    n_categories = sum(1 for k in ("pct_normal", "pct_warning", "pct_critical")
                        if who_stats[k] is not None and who_stats[k] > 0)

    return {
        "location_id": loc.id,
        "name": loc.name,
        "provider": loc.provider.name if loc.provider else "Unknown",
        "sensor_ids": [s.id for s in sensors],
        "window_start": window_start,
        "window_end": window_end,
        "n_obs": len(df),
        "n_days_classified": len(daily),
        "n_categories": n_categories,
        "completeness": round(len(df) / (WINDOW_DAYS * 24), 3),  # info saja, bukan syarat
        "pm25_min": df["pm25"].min(),
        "pm25_mean": df["pm25"].mean(),
        "pm25_p95": df["pm25"].quantile(0.95),
        "pm25_max": df["pm25"].max(),
        **who_stats,
        "raw": df,
    }


def select_stations(profiles):
    """Lolos jika n_obs >= 120 (dari desain eksperimen sendiri) DAN
    mencakup >= 2 kategori WHO. completeness tidak dipakai di sini."""
    eligible = [p for p in profiles
                if p["n_obs"] >= MIN_OBS and p["n_categories"] >= MIN_CATEGORIES]
    eligible.sort(key=lambda p: p["n_obs"], reverse=True)

    if len(eligible) <= N_STATIONS_MAX:
        return eligible

    chosen, covered = [], set()
    for p in eligible:
        cats = set()
        if p["pct_normal"]: cats.add("normal")
        if p["pct_warning"]: cats.add("warning")
        if p["pct_critical"]: cats.add("critical")
        if cats - covered:
            chosen.append(p)
            covered |= cats
        if len(chosen) == N_STATIONS_MAX:
            break
    for p in eligible:
        if len(chosen) >= N_STATIONS_MAX:
            break
        if p not in chosen:
            chosen.append(p)
    return chosen[:N_STATIONS_MAX]


def main():
    print("Ambil lokasi PM2.5 Indonesia...")
    locs = get_all_locations(get_indonesia_country_id())
    locs = [l for l in locs if in_bbox(l) and pm25_sensors(l)]
    print(f"Kandidat: {len(locs)}\n")

    profiles = []
    for i, loc in enumerate(locs, 1):
        try:
            p = profile_station(loc)
        except Exception as e:
            print(f"  [{i}/{len(locs)}] {loc.name}: gagal ({e})")
            continue
        if p is None:
            continue
        profiles.append(p)
        print(f"  [{i}/{len(locs)}] {loc.name}: n_obs={p['n_obs']} "
              f"kategori={p['n_categories']} sensors={p['sensor_ids']}")

    if not profiles:
        raise SystemExit("Tidak ada stasiun dengan data.")

    cols = ["name", "provider", "n_obs", "n_days_classified", "n_categories",
            "completeness", "pm25_min", "pm25_mean", "pm25_p95", "pm25_max",
            "pct_normal", "pct_warning", "pct_critical"]
    print("\n=== SEMUA KANDIDAT ===")
    print(pd.DataFrame(profiles)[cols].round(1).to_string(index=False))

    chosen = select_stations(profiles)
    if len(chosen) < N_STATIONS_MIN:
        print(f"\nPeringatan: cuma {len(chosen)} stasiun lolos n_obs>={MIN_OBS} "
              f"dan kategori>={MIN_CATEGORIES}.")

    print(f"\n=== {len(chosen)} STASIUN TERPILIH ===")
    print(pd.DataFrame(chosen)[cols].round(1).to_string(index=False))

    meta_df = pd.DataFrame(chosen)[[
        "location_id", "name", "provider", "sensor_ids",
        "window_start", "window_end", "n_obs", "completeness"
    ]].copy()
    meta_df["sensor_ids"] = meta_df["sensor_ids"].apply(lambda ids: ",".join(map(str, ids)))
    meta_df.to_csv(os.path.join(OUT_DIR, "stations_metadata.csv"), index=False)

    frames = []
    for p in chosen:
        df = p["raw"].copy()
        df.insert(0, "location", p["name"])
        df.insert(1, "sensor_ids", ",".join(map(str, p["sensor_ids"])))
        frames.append(df[["timestamp", "location", "sensor_ids", "pm25"]])
    pd.concat(frames, ignore_index=True).sort_values(["location", "timestamp"]) \
        .to_csv(os.path.join(OUT_DIR, "pm25_payload.csv"), index=False)

    print(f"\nDisimpan ke {OUT_DIR}/")
    client.close()


if __name__ == "__main__":
    main()