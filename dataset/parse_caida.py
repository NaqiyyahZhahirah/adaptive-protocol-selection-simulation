import csv, glob, gzip, os

INPUT_GLOB = "dataset/network_data.txt"
OUTPUT_CSV = "dataset/traces_parsed.csv"
MAX_TRACES_PER_FILE = None

def open_any(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "rt", encoding="utf-8", errors="replace")

def parse_hops(hop_fields):
    """Hitung hop terjawab, hop diam ('q'), total percobaan, dan percobaan gagal."""
    answered = silent = tries_total = tries_lost = 0
    for h in hop_fields:
        if h == "q":
            silent += 1
            continue
        for resp in h.split(";"):
            parts = resp.split(",")
            if len(parts) != 3:
                continue
            try:
                n = int(parts[2])
            except ValueError:
                continue
            answered += 1
            tries_total += n
            tries_lost += n - 1
    return answered, silent, tries_total, tries_lost

def main():
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    header = ["monitor", "dest", "ts", "replied", "rtt_ms",
              "hops_ok", "hops_silent", "tries_total", "tries_lost", "file"]
    n_total = 0
    with open(OUTPUT_CSV, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(header)
        for path in sorted(glob.glob(INPUT_GLOB)):
            n_file = 0
            with open_any(path) as f:
                for line in f:
                    if not line.startswith("T\t"):
                        continue
                    p = line.rstrip("\n").split("\t")
                    if len(p) < 13:
                        continue
                    try:
                        ts = int(p[5])
                        replied = 1 if p[6] == "R" else 0
                        rtt = float(p[7])
                    except ValueError:
                        continue
                    ok, silent, tt, tl = parse_hops(p[13:])
                    w.writerow([p[1], p[2], ts, replied,
                                rtt if replied else "",
                                ok, silent, tt, tl, os.path.basename(path)])
                    n_file += 1
                    if MAX_TRACES_PER_FILE and n_file >= MAX_TRACES_PER_FILE:
                        break
            n_total += n_file
            print(f"{os.path.basename(path)}: {n_file:,} trace")
    print(f"Selesai: {n_total:,} trace -> {OUTPUT_CSV}")

if __name__ == "__main__":
    main()