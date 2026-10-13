from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAYLOAD_CSV = ROOT / "OpenAQ-dataset" / "data" / "pm25_payload.csv"
RESULTS_DIR = ROOT / "results"
BROKER_CONF = ROOT / "simulation" / "broker-config" / "mosquitto.conf"

HOST = "127.0.0.1"
BROKER_PORT = 1884
COAP_SERVER_PORT = 5690
TCP_PORT_BASE = 20001      # injector TCP node ke-i = 20001 + i
UDP_PORT_BASE = 30001      # injector UDP node ke-i = 30001 + i
MQTT_TOPIC = "airquality"

NODES = ["Bogor Selatan", "Jakarta Central", "Wedomartani", "Tabanan", "Padang2 Uluwatu"]

# Komposisi 600 pesan: 120 per node, 200 per kategori (Tabel 3.7).
# Sesuaikan jika node berubah. Divalidasi saat build_messages().
ALLOCATION = {
    "Bogor Selatan":   {"normal": 40, "warning": 40, "critical": 40},
    "Jakarta Central": {"normal": 80, "warning": 40, "critical": 0},
    "Wedomartani":     {"normal": 0,  "warning": 40, "critical": 80},
    "Tabanan":         {"normal": 0,  "warning": 40, "critical": 80},
    "Padang2 Uluwatu": {"normal": 80, "warning": 40, "critical": 0},
}
DATA_SEED = 0          # sampel data tetap di semua run (variabel kontrol)
MIN_PERIODS = 18       # sama dengan fetch_pm25_openaq.py
WHO_AQG, WHO_IT3 = 15.0, 37.5

SEND_INTERVAL = 0.1    # detik per node (Tabel 3.7)
DYNAMIC_BLOCK = 60     # kondisi berganti tiap 60 pesan (global)
SEEDS = [1, 2, 3, 4, 5]

# Tabel 3.3
LEVELS = ["low", "medium", "high"]
RTT_MS = {"low": (0.0, 58.4), "medium": (58.4, 218.6), "high": (218.6, 358.8)}
LOSS_PCT = {"low": (0.0, 5.0), "medium": (5.0, 15.0), "high": (15.0, 30.0)}

# Emulasi loss TCP: penalti retransmisi (RFC 6298: RTO awal 1 s, backoff x2)
TCP_RTO_MIN = 1.0
TCP_MAX_TRIES = 5
IP_UDP_HDR = 28
IP_TCP_HDR = 40

MQTT_ACK_WAIT = 60.0
COAP_WAIT = {"CON": 120.0, "NON": 5.0}
DRAIN_TIMEOUT = 130.0  # CoAP CON bisa ~93 s sebelum menyerah

# Skema adaptif
WINDOW = 10
DEFAULT_SCORE = 50.0
# ACK yang lebih lambat dari ini dianggap "gagal pada percobaan pertama"
# (CoAP retransmit pertama >= 2 s, MQTT penalti pertama >= 1 s)
FIRST_TRY_S = {"mqtt": 1.0, "coap": 2.0}