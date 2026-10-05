import pandas as pd

df = pd.read_csv("OpenAQ-dataset/data/pm25_payload.csv", parse_dates=["timestamp"])

# Versi 1: hari kalender UTC (sama dengan fetch_pm25_openaq.py dan Tabel 3.2)
daily = (df.set_index("timestamp")
           .groupby("location")["pm25"]
           .resample("24h").mean().dropna())
print("P99 per hari kalender:", daily.quantile(0.99))

# Versi 2: trailing rolling 24 jam (sesuai Batasan Masalah di Bab I)
roll = (df.sort_values("timestamp")
          .set_index("timestamp")
          .groupby("location")["pm25"]
          .rolling("24h", min_periods=18).mean().dropna())
print("P99 rolling 24 jam:", roll.quantile(0.99))