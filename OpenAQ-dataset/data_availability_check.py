"""
Cari jendela 30 hari (mulai Jan 2025) di mana >= 5 stasiun PM2.5 Indonesia
punya coverage >= 90% (turun ke 85%, lalu 80% kalau perlu).
Coverage harian diambil dari endpoint agregat harian OpenAQ v3.
"""

import os
import time
import pandas as pd
from dotenv import load_dotenv
from openaq import OpenAQ

load_dotenv()
client = OpenAQ(api_key=os.getenv("OPENAQ_API_KEY"))

COUNTRY_CODE = "ID"
START = pd.Timestamp("2025-01-01", tz="UTC")
END = pd.Timestamp.now(tz="UTC").normalize()
WINDOW_DAYS = 30
THRESHOLDS = [0.90, 0.85, 0.80]   # dicoba dari yang paling ketat
MAX_BAD_DAYS = 3                  # maks hari "bolong" dalam jendela
BAD_DAY_LEVEL = 0.5               # hari dianggap bolong kalau coverage < 50%
TARGET = 5
SLEEP = 0.3
OUT_DIR = "coverage_scan"

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


def get_all_locations(country_id, page_limit=1000):
    out, page = [], 1
    while True:
        res = client.locations.list(countries_id=country_id, limit=page_limit, page=page)
        out.extend(res.results)
        if len(res.results) < page_limit:
            return out
        page += 1
        time.sleep(SLEEP)


def in_bbox(loc):
    lat, lon = loc.coordinates.latitude, loc.coordinates.longitude
    return BBOX_LAT[0] <= lat <= BBOX_LAT[1] and BBOX_LON[0] <= lon <= BBOX_LON[1]


def pm25_sensor(loc):
    return next((s for s in loc.sensors if s.parameter.name == "pm25"), None)


def still_relevant(loc):
    """Buang lokasi yang datanya sudah berhenti sebelum jendela pertama mungkin selesai."""
    last = to_utc(pick(loc, "datetime_last"))
    return last is None or last >= START + pd.Timedelta(days=WINDOW_DAYS)


def fetch_daily_coverage(sensor_id):
    """Return Series: tanggal -> fraksi coverage harian (0..1)."""
    rows, page = {}, 1

    while True:
        res = client.measurements.list(
            sensors_id=sensor_id,
            data="days",
            date_from=START.strftime("%Y-%m-%d"),
            date_to=END.strftime("%Y-%m-%d"),
            limit=1000,
            page=page,
        )

        for m in res.results:
            period = pick(m, "period")

            if period is None:
                continue

            dt = to_utc(
                pick(period, "datetime_from", "datetimeFrom")
            )

            cov = pick(m, "coverage")

            if dt is None or cov is None:
                continue

            # Prioritaskan percentCoverage
            pc = pick(
                cov,
                "percent_coverage",
                "percentCoverage",
                "percent_complete",
                "percentComplete",
            )

            # Kalau persentase tidak tersedia,
            # hitung dari observed / expected
            if pc is None:
                exp = pick(cov, "expected_count", "expectedCount")
                obs = pick(cov, "observed_count", "observedCount")

                if exp:
                    pc = 100 * obs / exp

            if pc is None:
                continue

            day = dt.tz_localize(None).normalize()

            rows[day] = max(
                rows.get(day, 0.0),
                min(float(pc) / 100, 1.0)
            )

        if len(res.results) < 1000:
            break

        page += 1
        time.sleep(SLEEP)

    return pd.Series(rows, dtype=float)


def scan(daily, thr):
    roll = daily.rolling(WINDOW_DAYS, min_periods=WINDOW_DAYS).mean()
    bad = (daily < BAD_DAY_LEVEL).astype(int).rolling(WINDOW_DAYS, min_periods=WINDOW_DAYS).sum()
    ok = (roll >= thr) & (bad <= MAX_BAD_DAYS)
    count = ok.sum(axis=1)

    best = None  # (window_end, score)
    for end, n in count.items():
        if n < TARGET:
            continue
        top = roll.loc[end][ok.loc[end]].sort_values(ascending=False).head(TARGET)
        score = top.mean()
        if best is None or score > best[1]:
            best = (end, score)
    return roll, bad, ok, count, best


def pick_diverse(qualified, meta):
    """Utamakan provider berbeda, sisanya isi dari coverage tertinggi."""
    chosen, seen = [], set()
    for lid in qualified.index:
        prov = meta[lid]["provider"]
        if prov not in seen:
            chosen.append(lid)
            seen.add(prov)
        if len(chosen) == TARGET:
            return chosen
    for lid in qualified.index:
        if lid not in chosen:
            chosen.append(lid)
            if len(chosen) == TARGET:
                break
    return chosen


def main():
    print("Ambil lokasi Indonesia...")
    locs = get_all_locations(get_indonesia_country_id())
    locs = [l for l in locs if in_bbox(l) and pm25_sensor(l) and still_relevant(l)]
    print(f"Kandidat (bbox + pm25 + belum mati sebelum {START.date()}+30 hari): {len(locs)}")

    meta, series = {}, {}
    for i, loc in enumerate(locs, 1):
        s = pm25_sensor(loc)
        print(f"  [{i}/{len(locs)}] {loc.name} (sensor {s.id})")
        try:
            ser = fetch_daily_coverage(s.id)
        except Exception as e:
            print(f"    ! gagal: {e}")
            continue
        if ser.empty:
            continue
        series[loc.id] = ser
        meta[loc.id] = {
            "name": loc.name,
            "provider": loc.provider.name if loc.provider else "Unknown",
            "sensor_id": s.id,
        }
        time.sleep(SLEEP)

    if not series:
        raise SystemExit("Tidak ada data coverage. Cek nama field: print(vars(res.results[0])).")

    idx = pd.date_range(START.tz_localize(None), END.tz_localize(None), freq="D")
    daily = pd.DataFrame({lid: s.reindex(idx).fillna(0.0) for lid, s in series.items()})
    daily.to_csv(os.path.join(OUT_DIR, "daily_coverage.csv"))

    result = None
    for thr in THRESHOLDS:
        roll, bad, ok, count, best = scan(daily, thr)
        print(f"\nAmbang {thr:.0%}: jendela terbaik punya maks {int(count.max())} stasiun lolos")
        if best:
            result = (thr, roll, bad, ok, best)
            break

    if result is None:
        top_end = count.idxmax()
        print(f"\nTidak ada jendela dengan {TARGET} stasiun bahkan di ambang {THRESHOLDS[-1]:.0%}.")
        print(f"Paling banyak: {int(count.max())} stasiun, jendela berakhir {top_end.date()}.")
        return

    thr, roll, bad, ok, (end, score) = result
    start = end - pd.Timedelta(days=WINDOW_DAYS - 1)
    qualified = roll.loc[end][ok.loc[end]].sort_values(ascending=False)
    chosen = pick_diverse(qualified, meta)

    print(f"\n=== JENDELA TERPILIH (ambang {thr:.0%}) ===")
    print(f"DATE_FROM = {start.date()}T00:00:00Z")
    print(f"DATE_TO   = {end.date()}T23:59:59Z")
    print(f"Stasiun lolos di jendela ini: {len(qualified)}")

    rows = []
    for lid in chosen:
        rows.append({
            "location_id": lid,
            "name": meta[lid]["name"],
            "provider": meta[lid]["provider"],
            "sensor_id": meta[lid]["sensor_id"],
            "coverage": round(float(roll.loc[end, lid]), 3),
            "bad_days": int(bad.loc[end, lid]),
        })
    out = pd.DataFrame(rows)
    print(f"\n=== {TARGET} STASIUN ===")
    print(out.to_string(index=False))
    out.to_csv(os.path.join(OUT_DIR, "selected_stations.csv"), index=False)

    client.close()


if __name__ == "__main__":
    main()