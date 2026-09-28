"""
Pengambilan data PM2.5 hourly dari OpenAQ API v3 untuk membentuk payload simulasi MQTT/CoAP.
Data ini adalah data observasi, BUKAN dataset training machine learning.
"""

import os
import re
import sys
import json
import time
import hashlib
from datetime import datetime, timezone

import requests
import pandas as pd
from dotenv import load_dotenv

# =====================================================================
# PARAMETER DESAIN PENELITIAN
# Semua angka di blok ini adalah KEPUTUSAN PENELITI, bukan ketentuan
# WHO, OpenAQ, atau Xu & Chen (2025).
# =====================================================================
COUNTRY_ISO = "ID"
PARAMETER_NAME = "pm25"           # nama parameter di OpenAQ (id dicari lewat /v3/parameters)
WINDOW_DAYS = 30                  # desain: panjang periode pengamatan
MIN_COVERAGE = 0.80               # desain: ambang QC coverage
TARGET_STATIONS_MIN = 3           # desain: batas bawah (hanya untuk peringatan)
TARGET_STATIONS_MAX = 5           # desain: batas atas, TIDAK dipaksakan tercapai

# Pemilihan periode bersama (desain):
TOP_K_WINDOWS = 5                 # berapa kandidat periode yang diverifikasi dengan data hourly
WINDOW_SEPARATION_DAYS = 7        # kandidat periode harus berjarak minimal ini (hindari yang nyaris identik)
WINDOW_START_OVERRIDE = None      # mis. "2025-03-01" -> kunci periode agar rerun identik

# Anti-duplikasi (desain):
ONE_SENSOR_PER_LOCATION = True    # maksimal 1 sensor terpilih per location_id
DUPLICATE_CORR_THRESHOLD = 0.99   # None = nonaktif. Korelasi hourly >= ini dianggap duplikasi
DUPLICATE_MIN_OVERLAP_HOURS = 168 # korelasi hanya dihitung jika irisan jam >= ini

# QC variasi OPSIONAL (desain, default nonaktif; lihat statistik dulu baru putuskan):
OPTIONAL_MIN_UNIQUE_VALUES = None # mis. 20
OPTIONAL_MIN_RANGE = None         # mis. 5.0 (satuan µg/m³)

# Teknis:
BASE_URL = "https://api.openaq.org/v3"
PAGE_LIMIT = 1000
SLEEP = 1.0                       # jeda antar request (detik)
OUT_DIR = "output"
DEBUG_PRINT_FIRST_HOURS_RECORD = True   # cetak 1 record /hours mentah untuk verifikasi nama field
# =====================================================================

load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    sys.exit("OPENAQ_API_KEY tidak ditemukan. Isi di file .env")

SESSION = requests.Session()
SESSION.headers.update({"X-API-Key": API_KEY})
ENDPOINTS_USED = set()
CACHE = {}
_debug_printed = False

W = pd.Timedelta(days=WINDOW_DAYS)
EXPECTED_HOURS = WINDOW_DAYS * 24


# ---------------------------------------------------------------- helper API
def api_get(path, params=None):
    ENDPOINTS_USED.add("GET " + BASE_URL + re.sub(r"/\d+", "/{id}", path))
    for attempt in range(6):
        r = SESSION.get(BASE_URL + path, params=params, timeout=60)
        if r.status_code == 429:
            wait = int(r.headers.get("x-ratelimit-reset") or r.headers.get("Retry-After") or 60)
            print(f"  rate limit, tunggu {wait}s...")
            time.sleep(wait + 1)
            continue
        if r.status_code >= 500:
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        time.sleep(SLEEP)
        return r.json()
    raise RuntimeError(f"Gagal setelah retry: {path}")


def to_utc(x):
    if x is None:
        return None
    if isinstance(x, dict):
        x = x.get("utc")
    return pd.to_datetime(x, utc=True) if x else None


def iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


# ------------------------------------------------ Tahap 1: kandidat sensor
def get_parameter_id():
    res = api_get("/parameters", {"limit": PAGE_LIMIT})["results"]
    match = [p for p in res if p.get("name") == PARAMETER_NAME]
    if not match:
        sys.exit(f"Parameter '{PARAMETER_NAME}' tidak ada di /v3/parameters")
    return match[0]["id"]


def collect_candidates(param_id):
    cands, n_locs, page = [], 0, 1
    while True:
        res = api_get("/locations", {"iso": COUNTRY_ISO, "parameters_id": param_id,
                                     "limit": PAGE_LIMIT, "page": page})["results"]
        for loc in res:
            n_locs += 1
            coords = loc.get("coordinates") or {}
            # SEMUA sensor PM2.5 di lokasi ini, bukan hanya yang pertama
            for s in loc.get("sensors") or []:
                p = s.get("parameter") or {}
                if p.get("id") == param_id:
                    cands.append({
                        "location_id": loc["id"],
                        "location_name": loc.get("name"),
                        "sensor_id": s["id"],
                        "parameter": p.get("name"),
                        "unit": p.get("units"),
                        "provider": (loc.get("provider") or {}).get("name"),
                        "lat": coords.get("latitude"),
                        "lon": coords.get("longitude"),
                    })
        if len(res) < PAGE_LIMIT:
            break
        page += 1
    return n_locs, cands


def enrich_sensor_dates(cands):
    """datetimeFirst/Last level SENSOR diambil dari /v3/sensors/{id}."""
    for i, c in enumerate(cands, 1):
        try:
            s = api_get(f"/sensors/{c['sensor_id']}")["results"][0]
            c["first"], c["last"] = to_utc(s.get("datetimeFirst")), to_utc(s.get("datetimeLast"))
        except Exception as e:
            c["first"] = c["last"] = None
            c["error"] = f"gagal metadata: {e}"
        if i % 25 == 0:
            print(f"  metadata sensor {i}/{len(cands)}")


# ------------------------------------------- Tahap 2: pemilihan periode bersama
def sensor_start_range(c):
    """Rentang start (hari UTC) agar [start, start+W) berada dalam [first, last]."""
    if c.get("first") is None or c.get("last") is None:
        return None
    s0, s1 = c["first"].ceil("D"), (c["last"] - W).floor("D")
    return (s0, s1) if s0 <= s1 else None


def candidate_windows(cands):
    ranges = {c["sensor_id"]: sensor_start_range(c) for c in cands}
    ranges = {k: v for k, v in ranges.items() if v}
    if not ranges:
        return []
    breakpoints = {b for r in ranges.values() for b in r}
    scored = [(sum(r[0] <= s <= r[1] for r in ranges.values()), s) for s in breakpoints]
    scored.sort(key=lambda x: (-x[0], -x[1].value))          # banyak sensor dulu, lalu paling baru
    chosen = []
    for n, s in scored:
        if all(abs((s - t).days) >= WINDOW_SEPARATION_DAYS for _, t in chosen):
            chosen.append((n, s))
        if len(chosen) >= TOP_K_WINDOWS:
            break
    return chosen


# ---------------------------------------------------- Tahap 3: data hourly + QC
def fetch_hourly(sensor_id, start):
    """GET /v3/sensors/{id}/hours -> DataFrame [datetime, pm25], dalam [start, start+W)."""
    global _debug_printed
    key = (sensor_id, start)
    if key in CACHE:
        return CACHE[key]
    end, rows, page = start + W, [], 1
    while True:
        res = api_get(f"/sensors/{sensor_id}/hours", {
            "datetime_from": iso(start), "datetime_to": iso(end),
            "limit": PAGE_LIMIT, "page": page})["results"]
        if DEBUG_PRINT_FIRST_HOURS_RECORD and res and not _debug_printed:
            print("\n[DEBUG] contoh record /hours mentah:\n", json.dumps(res[0], indent=2)[:1500], "\n")
            _debug_printed = True
        for m in res:
            rows.append((to_utc((m.get("period") or {}).get("datetimeFrom")), m.get("value")))
        if len(res) < PAGE_LIMIT:
            break
        page += 1
    df = pd.DataFrame(rows, columns=["datetime", "pm25"]).dropna()
    n_raw = len(df)
    df = df[(df["datetime"] >= start) & (df["datetime"] < end)]
    df = df[df["pm25"] >= 0]                                  # QC desain: PM2.5 negatif = tidak valid
    df = df.drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)
    CACHE[key] = (df, n_raw - len(df))
    return CACHE[key]


def evaluate(c, start):
    df, n_dropped = fetch_hourly(c["sensor_id"], start)
    v = df["pm25"]
    rec = {"observed_hours": len(df), "coverage": len(df) / EXPECTED_HOURS,
           "n_dropped": n_dropped, "n_unique": v.nunique()}
    rec.update({
        "mean": v.mean(), "median": v.median(), "min": v.min(), "max": v.max(),
        "std": v.std(), "q25": v.quantile(.25), "q75": v.quantile(.75), "q98": v.quantile(.98),
    } if len(v) else {k: float("nan") for k in
                      ["mean", "median", "min", "max", "std", "q25", "q75", "q98"]})
    return rec


def run_window(cands, start):
    recs = []
    end = start + W
    for c in cands:
        r = sensor_start_range(c)
        rec = {**c, "status": None, "reason": None}
        if r is None:
            rec.update(status="ditolak", reason=c.get("error") or "datetimeFirst/Last tidak tersedia/tidak cukup 30 hari")
        elif not (r[0] <= start <= r[1]):
            rec.update(status="ditolak", reason="metadata datetimeFirst/Last tidak mencakup periode terpilih")
        else:
            try:
                rec.update(evaluate(c, start))
                if rec["observed_hours"] == 0:
                    rec.update(status="ditolak", reason="tidak ada data hourly pada periode")
                elif rec["coverage"] < MIN_COVERAGE:
                    rec.update(status="ditolak",
                               reason=f"coverage {rec['coverage']:.1%} < {MIN_COVERAGE:.0%}")
                elif OPTIONAL_MIN_UNIQUE_VALUES and rec["n_unique"] < OPTIONAL_MIN_UNIQUE_VALUES:
                    rec.update(status="ditolak", reason=f"nilai unik {rec['n_unique']} < {OPTIONAL_MIN_UNIQUE_VALUES} (QC opsional)")
                elif OPTIONAL_MIN_RANGE and (rec["max"] - rec["min"]) < OPTIONAL_MIN_RANGE:
                    rec.update(status="ditolak", reason=f"range < {OPTIONAL_MIN_RANGE} (QC opsional)")
                else:
                    rec["status"] = "lolos_qc"
            except Exception as e:
                rec.update(status="ditolak", reason=f"error fetch: {e}")
        recs.append(rec)
    return recs


# ------------------------------------------------------- Tahap 4: seleksi akhir
def select_final(recs, start):
    passing = sorted([r for r in recs if r["status"] == "lolos_qc"],
                     key=lambda r: (-r["observed_hours"], -r["std"], r["sensor_id"]))
    selected = []
    for r in passing:
        if len(selected) >= TARGET_STATIONS_MAX:
            r.update(status="tidak_dipilih", reason=f"lolos QC, di luar kuota {TARGET_STATIONS_MAX} (ranking lebih rendah)")
            continue
        if ONE_SENSOR_PER_LOCATION and any(s["location_id"] == r["location_id"] for s in selected):
            r.update(status="tidak_dipilih", reason="lokasi sama dengan sensor yang sudah dipilih")
            continue
        if DUPLICATE_CORR_THRESHOLD is not None:
            a = fetch_hourly(r["sensor_id"], start)[0].set_index("datetime")["pm25"]
            dup = None
            for s in selected:
                b = fetch_hourly(s["sensor_id"], start)[0].set_index("datetime")["pm25"]
                j = pd.concat([a, b], axis=1, join="inner")
                if len(j) >= DUPLICATE_MIN_OVERLAP_HOURS and j.iloc[:, 0].corr(j.iloc[:, 1]) >= DUPLICATE_CORR_THRESHOLD:
                    dup = s["sensor_id"]
                    break
            if dup:
                r.update(status="tidak_dipilih", reason=f"mirip duplikasi dengan sensor {dup} (korelasi >= {DUPLICATE_CORR_THRESHOLD})")
                continue
        r["status"] = "dipilih"
        selected.append(r)
    return selected


# ----------------------------------------------------------------------- main
def sha256(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def main():
    os.makedirs(os.path.join(OUT_DIR, "sensors"), exist_ok=True)
    retrieved_at = datetime.now(timezone.utc)

    param_id = get_parameter_id()
    n_locs, cands = collect_candidates(param_id)
    print(f"Kandidat lokasi: {n_locs} | kandidat sensor {PARAMETER_NAME}: {len(cands)}")
    enrich_sensor_dates(cands)

    # --- periode
    if WINDOW_START_OVERRIDE:
        windows = [(None, pd.Timestamp(WINDOW_START_OVERRIDE, tz="UTC"))]
    else:
        windows = candidate_windows(cands)
    if not windows:
        sys.exit("Tidak ada periode 30 hari yang tercakup metadata sensor mana pun.")

    best = None
    for n_meta, start in windows:
        print(f"\nVerifikasi periode {start.date()} s/d {(start + W).date()} (sensor tercakup metadata: {n_meta})")
        recs = run_window(cands, start)
        n_pass = sum(r["status"] == "lolos_qc" for r in recs)
        tot = sum(r.get("observed_hours") or 0 for r in recs if r["status"] == "lolos_qc")
        print(f"  -> lolos QC: {n_pass}")
        key = (n_pass, tot, start.value)          # sensor lolos terbanyak, lalu total jam valid, lalu terbaru
        if best is None or key > best[0]:
            best = (key, start, recs)
    _, start, recs = best
    end = start + W
    if best[0][0] == 0:
        pd.DataFrame(recs).to_csv(os.path.join(OUT_DIR, "candidates_all.csv"), index=False)
        sys.exit("Tidak ada sensor yang lolos QC pada periode manapun. Lihat output/candidates_all.csv")

    selected = select_final(recs, start)
    if len(selected) < TARGET_STATIONS_MIN:
        print(f"\n[PERINGATAN] Hanya {len(selected)} sensor lolos (< {TARGET_STATIONS_MIN}). Tidak dipaksakan.")

    # --- output
    cols = ["location_id", "location_name", "sensor_id", "parameter", "unit", "provider",
            "datetime_first", "datetime_last", "selected_period_start", "selected_period_end",
            "expected_hours", "observed_hours", "coverage",
            "mean", "median", "min", "max", "std", "q25", "q75", "q98"]
    sel_rows = [{**r, "datetime_first": iso(r["first"]), "datetime_last": iso(r["last"]),
                 "selected_period_start": iso(start), "selected_period_end": iso(end),
                 "expected_hours": EXPECTED_HOURS} for r in selected]
    sel_df = pd.DataFrame(sel_rows)[cols]
    sel_path = os.path.join(OUT_DIR, "selected_stations.csv")
    sel_df.to_csv(sel_path, index=False)

    frames = []
    for r in selected:
        d = fetch_hourly(r["sensor_id"], start)[0].copy()
        d["location_id"], d["location_name"] = r["location_id"], r["location_name"]
        d["sensor_id"], d["unit"] = r["sensor_id"], r["unit"]
        d = d[["datetime", "location_id", "location_name", "sensor_id", "pm25", "unit"]]
        d.to_csv(os.path.join(OUT_DIR, "sensors", f"pm25_sensor_{r['sensor_id']}.csv"), index=False)
        frames.append(d)
    comb_path = os.path.join(OUT_DIR, "combined_pm25_hourly.csv")
    pd.concat(frames, ignore_index=True).to_csv(comb_path, index=False)   # jam kosong TIDAK diisi/interpolasi

    all_df = pd.DataFrame(recs)
    all_df.to_csv(os.path.join(OUT_DIR, "candidates_all.csv"), index=False)
    all_df[all_df["status"].isin(["ditolak", "tidak_dipilih"])].to_csv(
        os.path.join(OUT_DIR, "rejected_sensors.csv"), index=False)

    meta = {
        "retrieved_at_utc": retrieved_at.isoformat(),
        "api_base": BASE_URL, "endpoints_used": sorted(ENDPOINTS_USED),
        "country_iso": COUNTRY_ISO, "parameter": PARAMETER_NAME, "parameter_id": param_id,
        "units": sorted({r["unit"] for r in selected}),
        "period_start_utc": iso(start), "period_end_utc_exclusive": iso(end),
        "aggregation": "OpenAQ /sensors/{id}/hours (hourly, timestamp = period.datetimeFrom.utc)",
        "qc_config": {
            "WINDOW_DAYS": WINDOW_DAYS, "MIN_COVERAGE": MIN_COVERAGE,
            "TARGET_STATIONS_MIN": TARGET_STATIONS_MIN, "TARGET_STATIONS_MAX": TARGET_STATIONS_MAX,
            "TOP_K_WINDOWS": TOP_K_WINDOWS, "WINDOW_SEPARATION_DAYS": WINDOW_SEPARATION_DAYS,
            "ONE_SENSOR_PER_LOCATION": ONE_SENSOR_PER_LOCATION,
            "DUPLICATE_CORR_THRESHOLD": DUPLICATE_CORR_THRESHOLD,
            "DUPLICATE_MIN_OVERLAP_HOURS": DUPLICATE_MIN_OVERLAP_HOURS,
            "OPTIONAL_MIN_UNIQUE_VALUES": OPTIONAL_MIN_UNIQUE_VALUES,
            "OPTIONAL_MIN_RANGE": OPTIONAL_MIN_RANGE,
            "negative_values": "dibuang (keputusan desain)", "gap_handling": "tidak diisi/interpolasi",
        },
        "n_candidate_locations": n_locs, "n_candidate_sensors": len(cands),
        "n_pass_qc": int(sum(r["status"] in ("lolos_qc", "dipilih", "tidak_dipilih") for r in recs)),
        "selected_sensor_ids": [r["sensor_id"] for r in selected],
        "sha256": {"selected_stations.csv": sha256(sel_path), "combined_pm25_hourly.csv": sha256(comb_path)},
    }
    with open(os.path.join(OUT_DIR, "run_metadata.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    # --- laporan terminal
    pd.set_option("display.width", 250)
    print("\n" + "=" * 70)
    print(f"Kandidat lokasi        : {n_locs}")
    print(f"Kandidat sensor {PARAMETER_NAME:<5}  : {len(cands)}")
    print(f"Periode terpilih (UTC) : {start.date()} s/d {(end - pd.Timedelta(hours=1)).date()} ({WINDOW_DAYS} hari, {EXPECTED_HOURS} jam)")
    print(f"Lolos coverage >= {MIN_COVERAGE:.0%} : {meta['n_pass_qc']}")
    print(f"Sensor dipilih         : {len(selected)}")
    print("\n--- Semua yang lolos QC (ranking: observed_hours desc, std desc) ---")
    ok = all_df[all_df["status"].isin(["dipilih", "tidak_dipilih"])]
    show = ["status", "location_id", "location_name", "sensor_id", "observed_hours", "coverage",
            "mean", "median", "min", "max", "std", "q25", "q75", "q98", "reason"]
    if not ok.empty:
        print(ok[show].round(2).to_string(index=False))
    print("\n--- Alasan penolakan (ringkas) ---")
    rej = all_df[all_df["status"] == "ditolak"]["reason"].fillna("?").str.replace(r"[\d.]+%", "X%", regex=True)
    print(rej.value_counts().to_string())
    print(f"\nOutput tersimpan di ./{OUT_DIR}/")


if __name__ == "__main__":
    main()