import os
import time
from dotenv import load_dotenv
from openaq import OpenAQ

load_dotenv()

API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    raise SystemExit("Set env var OPENAQ_API_KEY!.")

client = OpenAQ(api_key=API_KEY)

COUNTRY_CODE = "ID"
SLEEP_BETWEEN_CALLS = 0.3
REQUIRED_PARAMS = {"pm25"}
TARGET_STATION_COUNT = 5

# bounding box kasar Indonesia
BBOX_LAT = (-11, 6)
BBOX_LON = (95, 141)


# ambil semua lokasi Indonesia

def get_indonesia_country_id():
    res = client.countries.list(limit=1000)
    for c in res.results:
        if c.code == COUNTRY_CODE:
            return c.id
    raise RuntimeError("Indonesia tidak ditemukan di daftar countries")


def get_all_locations(country_id, page_limit=1000):
    all_locations = []
    page = 1
    while True:
        res = client.locations.list(countries_id=country_id, limit=page_limit, page=page)
        all_locations.extend(res.results)
        if len(res.results) < page_limit:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_CALLS)
    return all_locations


# filter bounding box 

def is_in_indonesia_bbox(loc):
    lat, lon = loc.coordinates.latitude, loc.coordinates.longitude
    return BBOX_LAT[0] <= lat <= BBOX_LAT[1] and BBOX_LON[0] <= lon <= BBOX_LON[1]


# filter parameter wajib 

def has_required_params(loc):
    params = {s.parameter.name for s in loc.sensors}
    return REQUIRED_PARAMS.issubset(params)


# hitung volume data 

def get_sensor_observed_count(sensor_id):
    try:
        res = client.sensors.get(sensor_id)
        if not res.results:
            return 0
        cov = getattr(res.results[0], "coverage", None)
        if cov is None:
            return 0
        return getattr(cov, "observed_count", 0) or 0
    except Exception as e:
        print(f"    ! gagal ambil sensor {sensor_id}: {e}")
        return 0


def build_location_profile(loc):
    total_observed = 0
    for s in loc.sensors:
        total_observed += get_sensor_observed_count(s.id)
        time.sleep(SLEEP_BETWEEN_CALLS)

    return {
        "id": loc.id,
        "name": loc.name,
        "provider": loc.provider.name if loc.provider else "Unknown",
        "owner": loc.owner.name if loc.owner else "Unknown",
        "params": sorted({s.parameter.name for s in loc.sensors}),
        "total_observed": total_observed,
    }


# diversifikasi provider 

MIN_VOLUME_THRESHOLD = 20000 

def select_diverse_stations(profiles, target_count=TARGET_STATION_COUNT,
                             min_volume=MIN_VOLUME_THRESHOLD):
    by_provider = {}
    for p in profiles:
        by_provider.setdefault(p["provider"], []).append(p)
    for provider in by_provider:
        by_provider[provider].sort(key=lambda x: x["total_observed"], reverse=True)

    selected = []
    selected_ids = set()

    # hanya provider yang kandidat terbaiknya >= ambang minimum
    eligible_providers = [
        prov for prov in by_provider
        if by_provider[prov][0]["total_observed"] >= min_volume
    ]
    provider_order = sorted(
        eligible_providers,
        key=lambda prov: by_provider[prov][0]["total_observed"],
        reverse=True,
    )
    for provider in provider_order:
        best = by_provider[provider][0]
        selected.append(best)
        selected_ids.add(best["id"])
        if len(selected) >= target_count:
            return selected[:target_count]

    # isi slot sisa murni dari volume tertinggi
    remaining_pool = sorted(
        [p for p in profiles if p["id"] not in selected_ids],
        key=lambda x: x["total_observed"],
        reverse=True,
    )
    for p in remaining_pool:
        selected.append(p)
        selected_ids.add(p["id"])
        if len(selected) >= target_count:
            break

    return selected[:target_count]


def main():
    print("\nambil semua lokasi Indonesia...")
    country_id = get_indonesia_country_id()
    locations = get_all_locations(country_id)
    print(f"  Total lokasi mentah: {len(locations)}")

    print("\nfilter bounding box...")
    locations = [loc for loc in locations if is_in_indonesia_bbox(loc)]
    print(f"  Tersisa setelah filter bbox: {len(locations)}")

    print("filter parameter pm25...")
    locations = [loc for loc in locations if has_required_params(loc)]
    print(f"  Tersisa setelah filter parameter: {len(locations)}")

    print("\nhitung volume data per lokasi...")
    profiles = []
    for i, loc in enumerate(locations, 1):
        print(f"  [{i}/{len(locations)}] {loc.name} ({len(loc.sensors)} sensor)...")
        profiles.append(build_location_profile(loc))

    print("\n semua stasiun (urut volume tertinggi)")
    for p in sorted(profiles, key=lambda x: x["total_observed"], reverse=True):
        print(f"  {p['name']:<30} {p['provider']:<20} obs={p['total_observed']:<8} params={p['params']}")

    print("\npilih 5 stasiun final...")
    final_stations = select_diverse_stations(profiles, TARGET_STATION_COUNT)

    print(f"\n {TARGET_STATION_COUNT} stasiun terpilih")
    for i, s in enumerate(final_stations, 1):
        print(f"{i}. {s['name']} (id={s['id']}) - {s['provider']} - "
              f"obs={s['total_observed']} - params={s['params']}")

    client.close()
    return final_stations


if __name__ == "__main__":
    main()